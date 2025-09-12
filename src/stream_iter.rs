use core::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::{FusedStream, Stream};

#[must_use]
pub(crate) struct StreamIter<'a, 'cx, S> {
    stream: Pin<&'a mut S>,
    cx: &'a mut Context<'cx>,
}

impl<'a, 'cx, S> StreamIter<'a, 'cx, S> {
    pub(crate) fn new(stream: Pin<&'a mut S>, cx: &'a mut Context<'cx>) -> Self {
        Self { stream, cx }
    }

    pub(crate) fn extend_into(self, collection: &mut impl Extend<S::Item>)
    where
        S: FusedStream,
    {
        if !self.stream.is_terminated() {
            collection.extend(SiImpl(self))
        }
    }
}

struct SiImpl<'a, 'cx, S>(StreamIter<'a, 'cx, S>);

impl<S: Stream> Iterator for SiImpl<'_, '_, S> {
    type Item = S::Item;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.stream.as_mut().poll_next(self.0.cx) {
            Poll::Ready(next) => next,
            Poll::Pending => None,
        }
    }
}
