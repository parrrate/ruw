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
            collection.extend(self)
        }
    }
}

impl<'a, 'cx, S: Stream> Iterator for StreamIter<'a, 'cx, S> {
    type Item = S::Item;

    fn next(&mut self) -> Option<Self::Item> {
        match self.stream.as_mut().poll_next(self.cx) {
            Poll::Ready(next) => next,
            Poll::Pending => None,
        }
    }
}
