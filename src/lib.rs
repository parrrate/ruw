//! Read-Update-Write

#![no_std]

use core::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::{ready, FusedStream, Future};
use pin_project::pin_project;

use crate::stream_iter::StreamIter;

mod stream_iter;

/// Read-Update-Write system.
///
/// * Keeps two update tracks while [`Ruw::write`] is in progress
///     * Based on old state, applied if write fails
///     * Based on new state, applied if write succeeds
/// * On failed [`Ruw::read`], rejects all incoming actions that have already arrived (insta-ready on [`poll_next`])
/// * On failed [`Ruw::write`], rejects all actions that went into the new [`Ruw::State`]
/// * On failed [`Ruw::update`] on either of two update tracks, rejects that action
/// * All updates are supposed to be synchronous and in-memory
///
/// [`poll_next`]: futures_core::Stream::poll_next
#[must_use]
pub trait Ruw {
    /// Central type for RUW. In [`std`] and [`alloc`] contexts, should rely on [`Arc`] to reduce
    /// cloning overhead.
    ///
    /// [`std`]: https://doc.rust-lang.org/stable/std/
    ///
    /// [`alloc`]: https://doc.rust-lang.org/stable/alloc/
    ///
    /// [`Arc`]: https://doc.rust-lang.org/stable/std/sync/struct.Arc.html
    type State: Clone;

    /// A single change applied to [`Ruw::State`] by [`Ruw::update`].
    type Delta: Clone;

    /// Represents either I/O error ([`Ruw::read`] and [`Ruw::write`]) or update error.
    type Error;

    /// Something to report completion of one action.
    type TrackOne;

    /// Something to report completion of zero or more actions.
    type TrackMany: Default + Extend<Self::TrackOne>;

    /// Try asynchronously reading the state.
    ///
    /// This should almost never fail. An error here is considered temporarily fatal draining and
    /// rejecting the whole queue.
    fn read(&self) -> impl Future<Output = Result<Self::State, Self::Error>>;

    /// Try updating the state.
    ///
    /// This method is ran twice if [`Ruw::write`] is in process. Failure of either one is taken as
    /// failure of both.
    fn update(state: Self::State, delta: Self::Delta) -> Result<Self::State, Self::Error>;

    /// Try asynchronously writing the state. Takes previous state for audit/logging/consistency.
    ///
    /// Restoration of failed state is out of scope for the current version.
    fn write(
        &self,
        old: Self::State,
        new: Self::State,
    ) -> impl Future<Output = Result<(), Self::Error>>;

    /// Report success.
    ///
    /// There is no `accept_one` function because the implementation is optimistic about being able
    /// to fit many changes into one state transition.
    fn accept(track: Self::TrackMany);

    /// Report many failures.
    fn reject(track: Self::TrackMany, error: Self::Error);

    /// Convert [`Ruw::TrackOne`] to [`Ruw::TrackMany`].
    #[must_use]
    fn many(one: Self::TrackOne) -> Self::TrackMany {
        let mut track: Self::TrackMany = Default::default();
        track.extend(Some(one));
        track
    }

    /// Report one failure.
    ///
    /// Used when [`Ruw::update`] fails.
    fn reject_one(one: Self::TrackOne, error: Self::Error) {
        Self::reject(Self::many(one), error);
    }
}

/// Run [`Ruw`] daemon until the provided [`FusedStream`] is done.
pub async fn ruw<R: Ruw>(ruw: &R, incoming: impl FusedStream<Item = (R::Delta, R::TrackOne)>) {
    Ruwing::<R, _, _, _> {
        incoming,
        state: Default::default(),
        read: || ruw.read(),
        write: |old, new| ruw.write(old, new),
    }
    .await
}

fn update_or_reject<R: Ruw>(
    state: R::State,
    delta: R::Delta,
    track: R::TrackOne,
) -> Option<(R::State, R::TrackOne)> {
    match R::update(state, delta) {
        Ok(state) => Some((state, track)),
        Err(error) => {
            R::reject_one(track, error);
            None
        }
    }
}

#[pin_project]
#[must_use]
struct Reading<R: Ruw, Rf> {
    #[pin]
    future: Rf,
    item: Option<(R::Delta, R::TrackOne)>,
}

#[must_use]
struct HeadState<R: Ruw> {
    fallback: R::State,
    success: R::State,
}

#[must_use]
struct HsIter<'a, R: Ruw, I> {
    state: &'a mut HeadState<R>,
    iter: I,
}

impl<R: Ruw, I: Iterator<Item = (R::Delta, R::TrackOne)>> Iterator for HsIter<'_, R, I> {
    type Item = R::TrackOne;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (delta, track) = self.iter.next()?;
            let Some((fallback, track)) =
                update_or_reject::<R>(self.state.fallback.clone(), delta.clone(), track)
            else {
                continue;
            };
            let Some((success, track)) =
                update_or_reject::<R>(self.state.success.clone(), delta, track)
            else {
                continue;
            };
            self.state.fallback = fallback;
            self.state.success = success;
            break Some(track);
        }
    }
}

#[must_use]
struct Head<R: Ruw> {
    state: HeadState<R>,
    track: R::TrackMany,
}

impl<R: Ruw> Head<R> {
    fn fallback_tail(self, prev: R::State) -> Tail<R> {
        Tail {
            prev,
            state: TailState {
                next: self.state.fallback,
            },
            track: self.track,
        }
    }

    fn success_tail(self, next: R::State) -> Tail<R> {
        Tail {
            prev: next,
            state: TailState {
                next: self.state.success,
            },
            track: self.track,
        }
    }
}

impl<R: Ruw> Extend<(R::Delta, R::TrackOne)> for Head<R> {
    fn extend<T: IntoIterator<Item = (R::Delta, R::TrackOne)>>(&mut self, iter: T) {
        self.track.extend(HsIter::<R, _> {
            state: &mut self.state,
            iter: iter.into_iter(),
        })
    }
}

#[must_use]
struct TailState<R: Ruw> {
    next: R::State,
}

#[must_use]
struct TsIter<'a, R: Ruw, I> {
    state: &'a mut TailState<R>,
    iter: I,
}

impl<R: Ruw, I: Iterator<Item = (R::Delta, R::TrackOne)>> Iterator for TsIter<'_, R, I> {
    type Item = R::TrackOne;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (delta, track) = self.iter.next()?;
            let Some((state, track)) =
                update_or_reject::<R>(self.state.next.clone(), delta.clone(), track)
            else {
                continue;
            };
            self.state.next = state;
            break Some(track);
        }
    }
}

#[must_use]
struct Tail<R: Ruw> {
    prev: R::State,
    state: TailState<R>,
    track: R::TrackMany,
}

impl<R: Ruw> Tail<R> {
    fn new(prev: R::State, next: R::State, track: R::TrackOne) -> Self {
        Self {
            prev,
            state: TailState { next },
            track: R::many(track),
        }
    }

    fn into_write_state(self) -> WriteState<R> {
        WriteState {
            tail: Some(self),
            head: None,
        }
    }

    fn write<Write: WriteFn<R>>(&self, write: &Write) -> Write::Wf {
        write(self.prev.clone(), self.state.next.clone())
    }

    fn writing<Write: WriteFn<R>>(self, write: &Write) -> Writing<R, Write::Wf> {
        Writing {
            future: self.write(write),
            state: self.into_write_state(),
        }
    }

    fn into_state<Write: WriteFn<R>, Rf>(self, write: &Write) -> State<R, Rf, Write::Wf> {
        State::Write(self.writing(write))
    }
}

impl<R: Ruw> Extend<(R::Delta, R::TrackOne)> for Tail<R> {
    fn extend<T: IntoIterator<Item = (R::Delta, R::TrackOne)>>(&mut self, iter: T) {
        self.track.extend(TsIter::<R, _> {
            state: &mut self.state,
            iter: iter.into_iter(),
        })
    }
}

#[must_use]
struct WriteState<R: Ruw> {
    tail: Option<Tail<R>>,
    head: Option<Head<R>>,
}

impl<R: Ruw> WriteState<R> {
    fn next_tail(&mut self, r: Result<(), R::Error>) -> Option<Tail<R>> {
        let tail = self.tail.take()?;
        Some(match r {
            Ok(()) => {
                R::accept(tail.track);
                self.head.take()?.success_tail(tail.prev)
            }
            Err(error) => {
                R::reject(tail.track, error);
                self.head.take()?.fallback_tail(tail.prev)
            }
        })
    }
}

impl<R: Ruw> Extend<(R::Delta, R::TrackOne)> for WriteState<R> {
    fn extend<T: IntoIterator<Item = (R::Delta, R::TrackOne)>>(&mut self, iter: T) {
        let Some(tail) = &self.tail else {
            return;
        };
        let mut iter = iter.into_iter();
        loop {
            match &mut self.head {
                Some(head) => {
                    head.extend(iter);
                    break;
                }
                None => match iter.next() {
                    Some((delta, track)) => {
                        let Some((fallback, track)) =
                            update_or_reject::<R>(tail.prev.clone(), delta.clone(), track)
                        else {
                            continue;
                        };
                        let Some((success, track)) =
                            update_or_reject::<R>(tail.state.next.clone(), delta, track)
                        else {
                            continue;
                        };
                        self.head = Some(Head {
                            state: HeadState { fallback, success },
                            track: R::many(track),
                        });
                    }
                    None => {
                        break;
                    }
                },
            }
        }
    }
}

#[pin_project]
#[must_use]
struct Writing<R: Ruw, Wf> {
    #[pin]
    future: Wf,
    state: WriteState<R>,
}

#[derive(Default)]
#[pin_project(project = StateProj)]
#[must_use]
enum State<R: Ruw, Rf, Wf> {
    #[default]
    Stale,
    Read(#[pin] Reading<R, Rf>),
    Write(#[pin] Writing<R, Wf>),
}

trait ReadFn<R: Ruw>: Fn() -> Self::Rf {
    type Rf: Future<Output = Result<R::State, R::Error>>;
}

impl<R: Ruw, Rf: Future<Output = Result<R::State, R::Error>>, Read: Fn() -> Rf> ReadFn<R> for Read {
    type Rf = Rf;
}

trait WriteFn<R: Ruw>: Fn(R::State, R::State) -> Self::Wf {
    type Wf: Future<Output = Result<(), R::Error>>;
}

impl<R: Ruw, Wf: Future<Output = Result<(), R::Error>>, Write: Fn(R::State, R::State) -> Wf>
    WriteFn<R> for Write
{
    type Wf = Wf;
}

#[pin_project]
#[must_use]
struct Ruwing<R: Ruw, Read: ReadFn<R>, Write: WriteFn<R>, S> {
    #[pin]
    incoming: S,
    #[pin]
    state: State<R, Read::Rf, Write::Wf>,
    read: Read,
    write: Write,
}

#[must_use]
struct RejectMany<'a, R: Ruw> {
    track: &'a mut R::TrackMany,
}

impl<R: Ruw> Extend<(R::Delta, R::TrackOne)> for RejectMany<'_, R> {
    fn extend<T: IntoIterator<Item = (R::Delta, R::TrackOne)>>(&mut self, iter: T) {
        self.track.extend(iter.into_iter().map(|(_, track)| track))
    }
}

impl<
        R: Ruw,
        Read: ReadFn<R>,
        Write: WriteFn<R>,
        S: FusedStream<Item = (R::Delta, R::TrackOne)>,
    > Future for Ruwing<R, Read, Write, S>
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let mut incoming = this.incoming;
        let mut state = this.state;
        loop {
            match state.as_mut().project() {
                StateProj::Stale if incoming.is_terminated() => break Poll::Ready(()),
                StateProj::Stale => {
                    let Some(item) = ready!(incoming.as_mut().poll_next(cx)) else {
                        break Poll::Ready(());
                    };
                    let reading = Reading {
                        future: (this.read)(),
                        item: Some(item),
                    };
                    state.as_mut().set(State::Read(reading));
                }
                StateProj::Read(reading) => {
                    let reading = reading.project();
                    match ready!(reading.future.poll(cx)) {
                        Ok(prev) => {
                            let mut item = reading.item.take();
                            loop {
                                match item.take() {
                                    Some((delta, track)) => {
                                        if let Some((next, track)) =
                                            update_or_reject::<R>(prev.clone(), delta, track)
                                        {
                                            let mut tail =
                                                Tail::<R>::new(prev.clone(), next.clone(), track);
                                            StreamIter::new(incoming.as_mut(), cx)
                                                .extend_into(&mut tail);
                                            state.as_mut().set(tail.into_state(this.write));
                                            break;
                                        }
                                    }
                                    None if incoming.is_terminated() => {
                                        state.as_mut().set(State::Stale);
                                        return Poll::Ready(());
                                    }
                                    None => match incoming.as_mut().poll_next(cx) {
                                        Poll::Ready(Some(next)) => {
                                            item = Some(next);
                                        }
                                        Poll::Ready(None) => {
                                            state.as_mut().set(State::Stale);
                                            return Poll::Ready(());
                                        }
                                        Poll::Pending => {
                                            state.as_mut().set(State::Stale);
                                            return Poll::Pending;
                                        }
                                    },
                                }
                            }
                        }
                        Err(error) => {
                            let mut track = reading
                                .item
                                .take()
                                .map(|(_, track)| R::many(track))
                                .unwrap_or_default();
                            {
                                StreamIter::new(incoming.as_mut(), cx)
                                    .extend_into(&mut RejectMany::<R> { track: &mut track });
                            }
                            R::reject(track, error);
                            state.as_mut().set(State::Stale);
                        }
                    }
                }
                StateProj::Write(writing) => {
                    let writing = writing.project();
                    let wstate = writing.state;
                    StreamIter::new(incoming.as_mut(), cx).extend_into(wstate);
                    let new = match wstate.next_tail(ready!(writing.future.poll(cx))) {
                        Some(tail) => tail.into_state(this.write),
                        None => State::Stale,
                    };
                    state.as_mut().set(new);
                }
            }
        }
    }
}
