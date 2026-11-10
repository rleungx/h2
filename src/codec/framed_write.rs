use crate::codec::UserError;
use crate::codec::UserError::*;
use crate::frame::{self, Frame, FrameSize};
use crate::hpack;

use bytes::{Buf, BufMut, BytesMut};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_util::io::poll_write_buf;

use std::io::{self, Cursor};

// A macro to get around a method needing to borrow &mut self
macro_rules! limited_write_buf {
    ($self:expr) => {{
        let limit = $self.max_frame_size() + frame::HEADER_LEN;
        $self.buf.get_mut().limit(limit)
    }};
}

#[derive(Debug)]
pub struct FramedWrite<T, B> {
    /// Upstream `AsyncWrite`
    inner: T,
    final_flush_done: bool,

    encoder: Encoder<B>,
}

#[derive(Debug)]
struct Encoder<B> {
    /// HPACK encoder
    hpack: hpack::Encoder,

    /// Write buffer
    ///
    /// TODO: Should this be a ring buffer?
    buf: Cursor<BytesMut>,

    /// Next frame to encode
    next: Option<Next<B>>,

    /// Last data frame
    last_data_frame: Option<frame::Data<B>>,

    /// Max frame size, this is specified by the peer
    max_frame_size: FrameSize,

    /// Chain payloads bigger than this.
    chain_threshold: usize,

    /// Min buffer required to attempt to write a frame
    min_buffer_capacity: usize,
}

#[derive(Debug)]
enum Next<B> {
    Data(frame::Data<B>),
    DataWithControl(frame::Data<B>),
    Continuation(frame::Continuation),
    ContinuationWithControl(frame::Continuation, FrameSize),
    // Prevent ordinary frames from overtaking the separate control queue.
    Control,
}

/// Initialize the connection with this amount of write buffer.
///
/// The minimum MAX_FRAME_SIZE is 16kb, so always be able to send a HEADERS
/// frame that big.
const DEFAULT_BUFFER_CAPACITY: usize = 16 * 1_024;

const MAX_CONTROL_BUFFER_SIZE: usize = 16 * 1_024;

/// Chain payloads bigger than this when vectored I/O is enabled. The remote
/// will never advertise a max frame size less than this (well, the spec says
/// the max frame size can't be less than 16kb, so not even close).
const CHAIN_THRESHOLD: usize = 256;

/// Chain payloads bigger than this when vectored I/O is **not** enabled.
/// A larger value in this scenario will reduce the number of small and
/// fragmented data being sent, and hereby improve the throughput.
const CHAIN_THRESHOLD_WITHOUT_VECTORED_IO: usize = 1024;

// TODO: Make generic
impl<T, B> FramedWrite<T, B>
where
    T: AsyncWrite + Unpin,
    B: Buf,
{
    pub fn new(inner: T) -> FramedWrite<T, B> {
        let chain_threshold = if inner.is_write_vectored() {
            CHAIN_THRESHOLD
        } else {
            CHAIN_THRESHOLD_WITHOUT_VECTORED_IO
        };
        FramedWrite {
            inner,
            final_flush_done: false,
            encoder: Encoder {
                hpack: hpack::Encoder::default(),
                buf: Cursor::new(BytesMut::with_capacity(DEFAULT_BUFFER_CAPACITY)),
                next: None,
                last_data_frame: None,
                max_frame_size: frame::DEFAULT_MAX_FRAME_SIZE,
                chain_threshold,
                min_buffer_capacity: chain_threshold + frame::HEADER_LEN,
            },
        }
    }

    /// Returns `Ready` when `send` is able to accept a frame
    ///
    /// Calling this function may result in the current contents of the buffer
    /// to be flushed to `T`.
    pub fn poll_ready(&mut self, cx: &mut Context) -> Poll<io::Result<()>> {
        if !self.encoder.has_capacity() {
            // Try flushing
            ready!(self.flush(cx))?;

            if !self.encoder.has_capacity() {
                return Poll::Pending;
            }
        }

        Poll::Ready(Ok(()))
    }

    /// Returns whether a frame can be buffered without first flushing the
    /// underlying I/O object.
    pub(crate) fn has_capacity(&self) -> bool {
        self.encoder.has_capacity()
    }

    /// Buffer a frame.
    ///
    /// `poll_ready` must be called first to ensure that a frame may be
    /// accepted.
    pub fn buffer(&mut self, item: Frame<B>) -> Result<(), UserError> {
        self.encoder.buffer(item)
    }

    pub(crate) fn poll_ready_control(
        &mut self,
        cx: &mut Context,
        item: &Frame<B>,
    ) -> Poll<io::Result<()>> {
        if self.poll_ready(cx)?.is_ready() {
            return Poll::Ready(Ok(()));
        }

        let queued = if self.encoder.has_control() {
            self.encoder.hpack.scratch_mut().len()
        } else {
            0
        };
        if control_frame_capacity(item) > MAX_CONTROL_BUFFER_SIZE - queued {
            return Poll::Pending;
        }

        Poll::Ready(Ok(()))
    }

    /// Buffer a control frame after `poll_ready_control` returns ready.
    pub(crate) fn buffer_control(&mut self, item: Frame<B>) -> Result<(), UserError> {
        if self.has_capacity() {
            return self.encoder.buffer(item);
        }

        // No new header block can be encoded until `next` is cleared. Reuse
        // HPACK's idle scratch buffer for controls during that interval.
        if !self.encoder.has_control() {
            self.encoder.hpack.scratch_mut().clear();
            self.encoder.next = match self.encoder.next.take() {
                Some(Next::Data(frame)) => Some(Next::DataWithControl(frame)),
                Some(Next::Continuation(frame)) => Some(Next::ContinuationWithControl(
                    frame,
                    self.encoder.max_frame_size,
                )),
                None => Some(Next::Control),
                _ => unreachable!("already deferred"),
            };
        }
        let control = self.encoder.hpack.scratch_mut();
        assert!(control_frame_capacity(&item) <= MAX_CONTROL_BUFFER_SIZE - control.len());
        match item {
            Frame::Ping(frame) => frame.encode(control),
            Frame::Settings(frame) => frame.encode(control),
            Frame::Reset(frame) => frame.encode(control),
            Frame::GoAway(frame) => frame.encode(control),
            _ => unreachable!("expected control frame"),
        }

        Ok(())
    }

    /// Flush buffered data to the wire
    pub fn flush(&mut self, cx: &mut Context) -> Poll<io::Result<()>> {
        let span = tracing::trace_span!("FramedWrite::flush");
        let _e = span.enter();

        loop {
            while !self.encoder.is_empty() {
                let n = match self.encoder.next {
                    Some(Next::Data(ref mut frame))
                    | Some(Next::DataWithControl(ref mut frame)) => {
                        tracing::trace!(queued_data_frame = true);
                        let mut buf = (&mut self.encoder.buf).chain(frame.payload_mut());
                        ready!(poll_write_buf(Pin::new(&mut self.inner), cx, &mut buf))?
                    }
                    _ => {
                        tracing::trace!(queued_data_frame = false);
                        ready!(poll_write_buf(
                            Pin::new(&mut self.inner),
                            cx,
                            &mut self.encoder.buf
                        ))?
                    }
                };
                if n == 0 {
                    // No progress is possible; retrying would busy-loop.
                    tracing::trace!("write returned zero, but non-zero bytes remaining");
                    return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
                }
            }

            match self.encoder.unset_frame() {
                ControlFlow::Continue => (),
                ControlFlow::Break => break,
            }
        }

        if matches!(self.encoder.next, Some(Next::Control)) {
            let control = self.encoder.hpack.scratch_mut();
            while control.has_remaining() {
                let n = ready!(poll_write_buf(Pin::new(&mut self.inner), cx, control))?;
                if n == 0 {
                    return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
                }
            }
            control.clear();
            self.encoder.next = None;
        }

        tracing::trace!("flushing buffer");
        // Flush the upstream
        ready!(Pin::new(&mut self.inner).poll_flush(cx))?;

        Poll::Ready(Ok(()))
    }

    /// Close the codec
    pub fn shutdown(&mut self, cx: &mut Context) -> Poll<io::Result<()>> {
        if !self.final_flush_done {
            ready!(self.flush(cx))?;
            self.final_flush_done = true;
        }
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

fn control_frame_capacity<B>(frame: &Frame<B>) -> usize {
    let payload = match frame {
        Frame::Ping(_) => 8,
        Frame::Settings(_) => 7 * 6,
        Frame::Reset(_) => 4,
        Frame::GoAway(frame) => frame.debug_data().len().saturating_add(8),
        _ => unreachable!("expected control frame"),
    };
    frame::HEADER_LEN.saturating_add(payload)
}

#[must_use]
enum ControlFlow {
    Continue,
    Break,
}

impl<B> Encoder<B>
where
    B: Buf,
{
    #[inline(always)]
    fn unset_frame(&mut self) -> ControlFlow {
        // Clear internal buffer
        self.buf.set_position(0);
        self.buf.get_mut().clear();

        // The data frame has been written, so unset it
        match self.next.take() {
            Some(Next::Data(frame)) => {
                self.last_data_frame = Some(frame);
                debug_assert!(self.is_empty());
                ControlFlow::Break
            }
            Some(Next::DataWithControl(frame)) => {
                self.last_data_frame = Some(frame);
                self.next = Some(Next::Control);
                debug_assert!(self.is_empty());
                ControlFlow::Break
            }
            Some(Next::Continuation(frame)) => {
                let mut buf = limited_write_buf!(self);
                if let Some(continuation) = frame.encode(&mut buf) {
                    self.next = Some(Next::Continuation(continuation));
                }
                ControlFlow::Continue
            }
            Some(Next::ContinuationWithControl(frame, max_frame_size)) => {
                // The SETTINGS ACK follows the complete header block, so its
                // new frame limit must not affect this block's remaining frames.
                let limit = max_frame_size as usize + frame::HEADER_LEN;
                let mut buf = self.buf.get_mut().limit(limit);
                self.next = Some(match frame.encode(&mut buf) {
                    Some(continuation) => {
                        Next::ContinuationWithControl(continuation, max_frame_size)
                    }
                    None => Next::Control,
                });
                ControlFlow::Continue
            }
            Some(Next::Control) => {
                self.next = Some(Next::Control);
                ControlFlow::Break
            }
            None => ControlFlow::Break,
        }
    }

    fn buffer(&mut self, item: Frame<B>) -> Result<(), UserError> {
        // Ensure that we have enough capacity to accept the write.
        assert!(self.has_capacity());
        let span = tracing::trace_span!("FramedWrite::buffer", frame = ?item);
        let _e = span.enter();

        tracing::debug!(frame = ?item, "send");

        match item {
            Frame::Data(mut v) => {
                // Ensure that the payload is not greater than the max frame.
                let len = v.payload().remaining();

                if len > self.max_frame_size() {
                    return Err(PayloadTooBig);
                }

                if len >= self.chain_threshold {
                    let head = v.head();

                    // Encode the frame head to the buffer
                    head.encode(len, self.buf.get_mut());

                    if self.buf.get_ref().remaining() < self.chain_threshold {
                        let extra_bytes = self.chain_threshold - self.buf.remaining();
                        self.buf.get_mut().put(v.payload_mut().take(extra_bytes));
                    }

                    // Save the data frame
                    self.next = Some(Next::Data(v));
                } else {
                    v.encode_chunk(self.buf.get_mut());

                    // The chunk has been fully encoded, so there is no need to
                    // keep it around
                    assert_eq!(v.payload().remaining(), 0, "chunk not fully encoded");

                    // Save off the last frame...
                    self.last_data_frame = Some(v);
                }
            }
            Frame::Headers(v) => {
                let mut buf = limited_write_buf!(self);
                if let Some(continuation) = v.encode(&mut self.hpack, &mut buf) {
                    self.next = Some(Next::Continuation(continuation));
                }
            }
            Frame::PushPromise(v) => {
                let mut buf = limited_write_buf!(self);
                if let Some(continuation) = v.encode(&mut self.hpack, &mut buf) {
                    self.next = Some(Next::Continuation(continuation));
                }
            }
            Frame::Settings(v) => {
                v.encode(self.buf.get_mut());
                tracing::trace!(rem = self.buf.remaining(), "encoded settings");
            }
            Frame::GoAway(v) => {
                v.encode(self.buf.get_mut());
                tracing::trace!(rem = self.buf.remaining(), "encoded go_away");
            }
            Frame::Ping(v) => {
                v.encode(self.buf.get_mut());
                tracing::trace!(rem = self.buf.remaining(), "encoded ping");
            }
            Frame::WindowUpdate(v) => {
                v.encode(self.buf.get_mut());
                tracing::trace!(rem = self.buf.remaining(), "encoded window_update");
            }

            Frame::Priority(_) => {
                /*
                v.encode(self.buf.get_mut());
                tracing::trace!("encoded priority; rem={:?}", self.buf.remaining());
                */
                unimplemented!();
            }
            Frame::Reset(v) => {
                v.encode(self.buf.get_mut());
                tracing::trace!(rem = self.buf.remaining(), "encoded reset");
            }
        }

        Ok(())
    }

    fn has_capacity(&self) -> bool {
        self.next.is_none()
            && (self.buf.get_ref().capacity() - self.buf.get_ref().len()
                >= self.min_buffer_capacity)
    }

    fn is_empty(&self) -> bool {
        match self.next {
            Some(Next::Data(ref frame)) | Some(Next::DataWithControl(ref frame)) => {
                !frame.payload().has_remaining()
            }
            _ => !self.buf.has_remaining(),
        }
    }
}

impl<B> Encoder<B> {
    fn has_control(&self) -> bool {
        matches!(
            self.next,
            Some(Next::DataWithControl(_))
                | Some(Next::ContinuationWithControl(..))
                | Some(Next::Control)
        )
    }

    fn max_frame_size(&self) -> usize {
        self.max_frame_size as usize
    }
}

impl<T, B> FramedWrite<T, B> {
    /// Returns the max frame size that can be sent
    pub fn max_frame_size(&self) -> usize {
        self.encoder.max_frame_size()
    }

    /// Set the peer's max frame size.
    pub fn set_max_frame_size(&mut self, val: usize) {
        assert!(val <= frame::MAX_MAX_FRAME_SIZE as usize);
        self.encoder.max_frame_size = val as FrameSize;
    }

    /// Set the peer's header table size.
    pub fn set_header_table_size(&mut self, val: usize) {
        self.encoder.hpack.update_max_size(val);
    }

    /// Retrieve the last data frame that has been sent
    pub fn take_last_data_frame(&mut self) -> Option<frame::Data<B>> {
        self.encoder.last_data_frame.take()
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T: AsyncRead + Unpin, B> AsyncRead for FramedWrite<T, B> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

// We never project the Pin to `B`.
impl<T: Unpin, B> Unpin for FramedWrite<T, B> {}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::Arc;
    use std::task::{Wake, Waker};

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    struct TestIo {
        written: Vec<u8>,
        allowance: usize,
        vectored: bool,
    }

    impl AsyncWrite for TestIo {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            if self.allowance == 0 {
                return Poll::Pending;
            }
            let len = buf.len().min(self.allowance).min(7);
            self.written.extend_from_slice(&buf[..len]);
            self.allowance -= len;
            Poll::Ready(Ok(len))
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[io::IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            let buf = bufs.iter().find(|buf| !buf.is_empty());
            self.poll_write(cx, buf.map_or(&[], |buf| &buf[..]))
        }

        fn is_write_vectored(&self) -> bool {
            self.vectored
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl FramedWrite<TestIo, Bytes> {
        fn send_control(&mut self, cx: &mut Context, frame: Frame<Bytes>) -> Result<(), UserError> {
            assert!(matches!(
                self.poll_ready_control(cx, &frame),
                Poll::Ready(Ok(()))
            ));
            self.buffer_control(frame)
        }
    }

    fn writer(vectored: bool) -> FramedWrite<TestIo, Bytes> {
        FramedWrite::new(TestIo {
            written: Vec::new(),
            allowance: 0,
            vectored,
        })
    }

    fn wire_frames(mut wire: &[u8]) -> Vec<(u8, u8, &[u8])> {
        let mut frames = Vec::new();
        while !wire.is_empty() {
            assert!(wire.len() >= frame::HEADER_LEN);
            let len =
                (usize::from(wire[0]) << 16) | (usize::from(wire[1]) << 8) | usize::from(wire[2]);
            let end = frame::HEADER_LEN + len;
            assert!(wire.len() >= end);
            frames.push((wire[3], wire[4], &wire[frame::HEADER_LEN..end]));
            wire = &wire[end..];
        }
        frames
    }

    #[test]
    fn deferred_controls_follow_partial_data_in_order() {
        let waker = Waker::from(Arc::new(NoopWake));
        let mut cx = Context::from_waker(&waker);

        for vectored in [false, true] {
            let mut writer = writer(vectored);
            let payload = Bytes::from(vec![42; 4096]);
            writer
                .buffer(frame::Data::new(1.into(), payload.clone()).into())
                .unwrap();
            writer.get_mut().allowance = 5;
            writer
                .send_control(&mut cx, frame::Ping::pong([1; 8]).into())
                .unwrap();
            writer
                .send_control(&mut cx, frame::Settings::ack().into())
                .unwrap();
            writer
                .send_control(
                    &mut cx,
                    frame::Reset::new(3.into(), frame::Reason::CANCEL).into(),
                )
                .unwrap();
            assert!(writer.poll_ready(&mut cx).is_pending());

            // Stop after the DATA frame and part of the first PING, then append
            // another PING while the control queue has a partially sent prefix.
            writer.get_mut().allowance = frame::HEADER_LEN + payload.len() - 5 + 7;
            assert!(writer.flush(&mut cx).is_pending());
            writer
                .send_control(&mut cx, frame::Ping::pong([2; 8]).into())
                .unwrap();
            writer.get_mut().allowance = usize::MAX;
            assert!(matches!(writer.flush(&mut cx), Poll::Ready(Ok(()))));

            let frames = wire_frames(&writer.inner.written);
            assert_eq!(frames.len(), 5);
            assert_eq!(frames[0], (0, 0, payload.as_ref()));
            assert_eq!(frames[1], (6, 1, &[1; 8][..]));
            assert_eq!(frames[2], (4, 1, &[][..]));
            assert_eq!(frames[3].0, 3);
            assert_eq!(frames[4], (6, 1, &[2; 8][..]));
            assert!(writer.has_capacity());
            assert_eq!(writer.take_last_data_frame().unwrap().payload().len(), 0);
        }
    }

    #[test]
    fn deferred_control_follows_all_continuations() {
        let waker = Waker::from(Arc::new(NoopWake));
        let mut cx = Context::from_waker(&waker);
        let mut writer = writer(false);
        let mut fields = http::HeaderMap::new();
        fields.insert(
            "large",
            http::HeaderValue::from_bytes(&vec![b'x'; 65536]).unwrap(),
        );
        writer
            .buffer(frame::Headers::new(1.into(), frame::Pseudo::default(), fields).into())
            .unwrap();
        assert!(matches!(writer.encoder.next, Some(Next::Continuation(..))));
        writer.get_mut().allowance = 5;
        writer
            .send_control(&mut cx, frame::Ping::pong([3; 8]).into())
            .unwrap();
        writer.get_mut().allowance = usize::MAX;
        assert!(matches!(writer.flush(&mut cx), Poll::Ready(Ok(()))));

        let frames = wire_frames(&writer.inner.written);
        assert!(frames.len() >= 3);
        assert_eq!(frames[0].0, 1);
        assert_eq!(frames[0].1 & 4, 0);
        let continuation_end = frames.len() - 2;
        for (index, frame) in frames[1..=continuation_end].iter().enumerate() {
            assert_eq!(frame.0, 9);
            assert_eq!(
                frame.1 & 4,
                if index + 1 == continuation_end { 4 } else { 0 }
            );
        }
        assert_eq!(frames.last().unwrap(), &(6, 1, &[3; 8][..]));
    }

    #[test]
    fn final_continuation_keeps_controls_before_new_frames() {
        let waker = Waker::from(Arc::new(NoopWake));
        let mut cx = Context::from_waker(&waker);
        let mut writer = writer(false);
        let mut fields = http::HeaderMap::new();
        fields.insert(
            "large",
            http::HeaderValue::from_bytes(&vec![b'x'; 32768]).unwrap(),
        );
        writer
            .buffer(frame::Headers::new(1.into(), frame::Pseudo::default(), fields).into())
            .unwrap();
        let first_frame_len = writer.encoder.buf.get_ref().len();
        writer
            .send_control(&mut cx, frame::Ping::pong([7; 8]).into())
            .unwrap();

        // Pause inside the final CONTINUATION, which leaves room in the normal
        // buffer but must not let a new frame overtake the deferred PING.
        writer.get_mut().allowance = first_frame_len + 5;
        assert!(writer.flush(&mut cx).is_pending());
        assert_eq!(&writer.inner.written[first_frame_len + 3..], &[9, 4]);
        assert!(!writer.has_capacity());
        assert!(writer.poll_ready(&mut cx).is_pending());

        writer.get_mut().allowance = usize::MAX;
        assert!(matches!(writer.poll_ready(&mut cx), Poll::Ready(Ok(()))));
        writer
            .buffer(frame::Data::new(1.into(), Bytes::from_static(b"tail")).into())
            .unwrap();
        assert!(matches!(writer.flush(&mut cx), Poll::Ready(Ok(()))));

        let frames = wire_frames(&writer.inner.written);
        assert_eq!(frames.len(), 4);
        assert_eq!(frames[0].0, 1);
        assert_eq!(frames[1].0, 9);
        assert_eq!(frames[1].1 & 4, 4);
        assert_eq!(frames[2], (6, 1, &[7; 8][..]));
        assert_eq!(frames[3], (0, 0, &b"tail"[..]));
    }

    #[test]
    fn deferred_controls_follow_full_ordinary_buffer() {
        let waker = Waker::from(Arc::new(NoopWake));
        let mut cx = Context::from_waker(&waker);
        let mut writer = writer(false);
        let mut ordinary_count = 0;
        while writer.has_capacity() {
            writer.buffer(frame::Ping::pong([8; 8]).into()).unwrap();
            ordinary_count += 1;
        }
        assert!(writer.encoder.next.is_none());
        let ordinary_len = writer.encoder.buf.get_ref().len();
        writer
            .send_control(&mut cx, frame::Ping::pong([9; 8]).into())
            .unwrap();

        // Finish the normal buffer, then stop partway through its deferred PING.
        writer.get_mut().allowance = ordinary_len + 5;
        assert!(writer.flush(&mut cx).is_pending());
        assert!(!writer.has_capacity());
        assert!(writer.poll_ready(&mut cx).is_pending());
        writer.get_mut().allowance = usize::MAX;
        assert!(matches!(writer.poll_ready(&mut cx), Poll::Ready(Ok(()))));
        writer
            .buffer(frame::Data::new(1.into(), Bytes::from_static(b"tail")).into())
            .unwrap();
        assert!(matches!(writer.flush(&mut cx), Poll::Ready(Ok(()))));

        let frames = wire_frames(&writer.inner.written);
        assert_eq!(frames.len(), ordinary_count + 2);
        for frame in &frames[..ordinary_count] {
            assert_eq!(*frame, (6, 1, &[8; 8][..]));
        }
        assert_eq!(frames[ordinary_count], (6, 1, &[9; 8][..]));
        assert_eq!(frames[ordinary_count + 1], (0, 0, &b"tail"[..]));
    }

    #[test]
    fn deferred_continuations_keep_original_frame_size() {
        let waker = Waker::from(Arc::new(NoopWake));
        let mut cx = Context::from_waker(&waker);
        let mut writer = writer(false);
        let old_max = writer.max_frame_size();
        let new_max = 64 * 1024;
        let mut fields = http::HeaderMap::new();
        fields.insert(
            "large",
            http::HeaderValue::from_bytes(&vec![b'x'; 128 * 1024]).unwrap(),
        );
        writer
            .buffer(frame::Headers::new(1.into(), frame::Pseudo::default(), fields.clone()).into())
            .unwrap();
        writer.get_mut().allowance = 5;
        writer
            .send_control(&mut cx, frame::Settings::ack().into())
            .unwrap();
        writer.set_max_frame_size(new_max);
        writer.get_mut().allowance = usize::MAX;
        assert!(matches!(writer.flush(&mut cx), Poll::Ready(Ok(()))));

        let frames = wire_frames(&writer.inner.written);
        assert!(frames.len() >= 3);
        assert_eq!(frames[0].0, 1);
        for frame in &frames[..frames.len() - 1] {
            assert!(frame.2.len() <= old_max);
        }
        assert_eq!(frames.last().unwrap(), &(4, 1, &[][..]));

        let next_block = writer.inner.written.len();
        writer
            .buffer(frame::Headers::new(3.into(), frame::Pseudo::default(), fields).into())
            .unwrap();
        assert!(matches!(writer.flush(&mut cx), Poll::Ready(Ok(()))));
        let frames = wire_frames(&writer.inner.written[next_block..]);
        assert_eq!(frames[0].0, 1);
        assert!(frames[0].2.len() > old_max);
        assert!(frames.iter().all(|frame| frame.2.len() <= new_max));
    }

    #[test]
    fn full_control_queue_preserves_pending_frame_until_writable() {
        let waker = Waker::from(Arc::new(NoopWake));
        let mut cx = Context::from_waker(&waker);
        let mut writer = writer(false);
        writer
            .buffer(frame::Data::new(1.into(), Bytes::from(vec![42; 4096])).into())
            .unwrap();

        let ping_count = MAX_CONTROL_BUFFER_SIZE / (frame::HEADER_LEN + 8);
        for _ in 0..ping_count {
            writer
                .send_control(&mut cx, frame::Ping::pong([4; 8]).into())
                .unwrap();
        }
        let pending = Frame::from(frame::Ping::pong([5; 8]));
        let queued = writer.encoder.hpack.scratch_mut().clone();
        for _ in 0..2 {
            assert!(writer.poll_ready_control(&mut cx, &pending).is_pending());
            assert_eq!(writer.encoder.hpack.scratch_mut(), &queued);
        }
        let goaway = Frame::from(frame::GoAway::new(
            frame::StreamId::zero(),
            frame::Reason::NO_ERROR,
        ));
        assert!(writer.poll_ready_control(&mut cx, &goaway).is_pending());
        assert_eq!(writer.encoder.hpack.scratch_mut(), &queued);
        assert!(writer.encoder.hpack.scratch_mut().remaining() <= MAX_CONTROL_BUFFER_SIZE);

        writer.get_mut().allowance = usize::MAX;
        assert!(matches!(
            writer.poll_ready_control(&mut cx, &pending),
            Poll::Ready(Ok(()))
        ));
        writer.buffer_control(pending).unwrap();
        assert!(matches!(writer.flush(&mut cx), Poll::Ready(Ok(()))));

        let frames = wire_frames(&writer.inner.written);
        assert_eq!(frames.len(), ping_count + 2);
        for frame in &frames[1..=ping_count] {
            assert_eq!(*frame, (6, 1, &[4; 8][..]));
        }
        assert_eq!(frames.last().unwrap(), &(6, 1, &[5; 8][..]));
    }

    #[test]
    fn ready_control_does_not_allocate_deferred_buffer() {
        let waker = Waker::from(Arc::new(NoopWake));
        let mut cx = Context::from_waker(&waker);
        let mut writer = writer(false);
        writer
            .send_control(&mut cx, frame::Ping::pong([6; 8]).into())
            .unwrap();
        assert!(!writer.encoder.has_control());
        writer.get_mut().allowance = usize::MAX;
        assert!(matches!(writer.flush(&mut cx), Poll::Ready(Ok(()))));
        assert_eq!(
            wire_frames(&writer.inner.written),
            vec![(6, 1, &[6; 8][..])]
        );
    }

    #[test]
    fn deferred_controls_can_reuse_header_scratch() {
        let waker = Waker::from(Arc::new(NoopWake));
        let mut cx = Context::from_waker(&waker);
        let mut writer = writer(false);
        let mut fields = http::HeaderMap::new();
        fields.insert("example", http::HeaderValue::from_static("value"));
        writer.get_mut().allowance = usize::MAX;
        writer
            .buffer(frame::Headers::new(1.into(), frame::Pseudo::default(), fields.clone()).into())
            .unwrap();
        assert!(matches!(writer.flush(&mut cx), Poll::Ready(Ok(()))));
        assert!(!writer.encoder.hpack.scratch_mut().is_empty());

        writer.get_mut().allowance = 0;
        writer
            .buffer(frame::Data::new(1.into(), Bytes::from(vec![42; 4096])).into())
            .unwrap();
        writer
            .send_control(&mut cx, frame::Ping::pong([9; 8]).into())
            .unwrap();
        writer.get_mut().allowance = usize::MAX;
        assert!(matches!(writer.flush(&mut cx), Poll::Ready(Ok(()))));
        writer
            .buffer(frame::Headers::new(3.into(), frame::Pseudo::default(), fields).into())
            .unwrap();
        assert!(matches!(writer.flush(&mut cx), Poll::Ready(Ok(()))));

        let frames = wire_frames(&writer.inner.written);
        assert_eq!(frames.len(), 4);
        assert_eq!(frames[0].0, 1);
        assert_eq!(frames[1], (0, 0, &vec![42; 4096][..]));
        assert_eq!(frames[2], (6, 1, &[9; 8][..]));
        assert_eq!(frames[3].0, 1);
    }

    #[test]
    fn borrowed_data_preserves_drop_checking() {
        let mut writer = FramedWrite::new(TestIo {
            written: Vec::new(),
            allowance: 0,
            vectored: false,
        });
        // Declaring the payload after the writer was already accepted before
        // deferred controls were supported, even though it is dropped first.
        let payload = vec![42; 4096];
        writer
            .buffer(frame::Data::new(1.into(), payload.as_slice()).into())
            .unwrap();
    }
    #[test]
    fn ordinary_continuations_use_updated_frame_size() {
        let waker = Waker::from(Arc::new(NoopWake));
        let mut cx = Context::from_waker(&waker);
        for (old_max, new_max) in [(16_384, 65_536), (65_536, 16_384)] {
            let mut writer = writer(false);
            writer.set_max_frame_size(old_max);
            let mut fields = http::HeaderMap::new();
            fields.insert(
                "large",
                http::HeaderValue::from_bytes(&vec![b'x'; 128 * 1024]).unwrap(),
            );
            writer
                .buffer(frame::Headers::new(1.into(), frame::Pseudo::default(), fields).into())
                .unwrap();
            writer.get_mut().allowance = 5;
            assert!(writer.flush(&mut cx).is_pending());
            writer.set_max_frame_size(new_max);
            writer.get_mut().allowance = usize::MAX;
            assert!(matches!(writer.flush(&mut cx), Poll::Ready(Ok(()))));

            let frames = wire_frames(&writer.inner.written);
            assert!(frames.len() >= 3);
            assert_eq!(frames[0].0, 1);
            assert!(frames[0].2.len() <= old_max);
            assert!(frames[1..]
                .iter()
                .all(|frame| frame.0 == 9 && frame.2.len() <= new_max));
            assert!(frames[1..].iter().any(|frame| frame.2.len() == new_max));
            assert_eq!(frames.last().unwrap().1 & 4, 4);
        }
    }

    #[test]
    fn deferred_controls_drain_between_episodes() {
        let waker = Waker::from(Arc::new(NoopWake));
        let mut cx = Context::from_waker(&waker);
        let mut writer = writer(false);
        for byte in 0..3 {
            writer.get_mut().allowance = 0;
            writer
                .buffer(frame::Data::new(1.into(), Bytes::from(vec![byte; 4096])).into())
                .unwrap();
            writer
                .send_control(&mut cx, frame::Ping::pong([byte; 8]).into())
                .unwrap();
            writer.get_mut().allowance = usize::MAX;
            assert!(matches!(writer.flush(&mut cx), Poll::Ready(Ok(()))));
            assert!(writer.has_capacity());
            assert_eq!(writer.take_last_data_frame().unwrap().payload().len(), 0);
        }
        let frames = wire_frames(&writer.inner.written);
        assert_eq!(frames.len(), 6);
        for (byte, pair) in frames.chunks_exact(2).enumerate() {
            assert_eq!(pair[0], (0, 0, &vec![byte as u8; 4096][..]));
            assert_eq!(pair[1], (6, 1, &[byte as u8; 8][..]));
        }
    }

    #[test]
    fn dropping_pending_writer_releases_data_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Debug)]
        struct Tracked {
            data: Bytes,
            drops: Arc<AtomicUsize>,
        }
        impl Buf for Tracked {
            fn remaining(&self) -> usize {
                self.data.remaining()
            }
            fn chunk(&self) -> &[u8] {
                self.data.chunk()
            }
            fn advance(&mut self, cnt: usize) {
                self.data.advance(cnt);
            }
        }
        impl Drop for Tracked {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::Relaxed);
            }
        }

        let waker = Waker::from(Arc::new(NoopWake));
        let mut cx = Context::from_waker(&waker);
        // Drop ordinary DATA, deferred DATA, and a control-only remainder.
        for (defer, allowance) in [(false, 0), (true, 0), (true, 4105)] {
            let drops = Arc::new(AtomicUsize::new(0));
            let mut writer = FramedWrite::new(TestIo {
                written: Vec::new(),
                allowance: 0,
                vectored: false,
            });
            writer
                .buffer(
                    frame::Data::new(
                        1.into(),
                        Tracked {
                            data: Bytes::from(vec![42; 4096]),
                            drops: drops.clone(),
                        },
                    )
                    .into(),
                )
                .unwrap();
            if defer {
                let frame = frame::Ping::pong([1; 8]).into();
                assert!(matches!(
                    writer.poll_ready_control(&mut cx, &frame),
                    Poll::Ready(Ok(()))
                ));
                writer.buffer_control(frame).unwrap();
            }
            writer.get_mut().allowance = allowance;
            assert!(writer.flush(&mut cx).is_pending());
            assert_eq!(drops.load(Ordering::Relaxed), 0);
            drop(writer);
            assert_eq!(drops.load(Ordering::Relaxed), 1);
        }
    }
}

#[cfg(feature = "unstable")]
mod unstable {
    use super::*;

    impl<T, B> FramedWrite<T, B> {
        pub fn get_ref(&self) -> &T {
            &self.inner
        }
    }
}
