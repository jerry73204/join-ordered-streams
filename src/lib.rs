//! Combines streams of ordered items into an ordered stream.

use educe::Educe;
use futures::{
    ready,
    stream::{self, FusedStream, FuturesUnordered},
    FutureExt, Stream, StreamExt,
};
use std::{cmp::Reverse, collections::BinaryHeap, task::Poll::*};

pub fn join_ordered_streams<K, S, I, F>(key_fn: F, streams: I) -> impl Stream<Item = S::Item> + Send
where
    K: Ord + Send,
    I: IntoIterator<Item = S> + Send,
    F: Fn(&S::Item) -> K + Sync + Send,
    S: FusedStream + Unpin + Send,
    S::Item: Send,
{
    async move {
        let futures: FuturesUnordered<_> = streams
            .into_iter()
            .map(|mut stream| async {
                let item = stream.next().await?;
                let key = key_fn(&item);
                Some(Entry {
                    item,
                    key: Reverse(key),
                    stream,
                })
            })
            .collect();

        let entries: BinaryHeap<_> = futures.map(stream::iter).flatten().collect().await;
        (key_fn, entries)
    }
    .into_stream()
    .flat_map(|(key_fn, mut entries)| {
        let mut pending: Option<PendingEntry<K, S>> = None;

        stream::poll_fn(move |cx| loop {
            if let Some(pending_) = &mut pending {
                let PendingEntry { stream, last_key } = pending_;

                let item = match ready!(stream.poll_next_unpin(cx)) {
                    Some(item) => item,
                    None => {
                        pending = None;
                        continue;
                    }
                };
                let next_key = key_fn(&item);

                if next_key > *last_key {
                    let stream = pending.take().unwrap().stream;
                    let entry = Entry {
                        key: Reverse(next_key),
                        item,
                        stream,
                    };
                    entries.push(entry);
                    continue;
                } else {
                    // TODO: warn the key goes backward
                    return Ready(Some(item));
                }
            }

            let Entry {
                key: Reverse(key),
                item,
                stream,
            } = match entries.pop() {
                Some(entry) => entry,
                None => return Ready(None),
            };

            pending = Some(PendingEntry {
                last_key: key,
                stream,
            });
            cx.waker().wake_by_ref();
            return Ready(Some(item));
        })
    })
}

#[derive(Educe)]
#[educe(PartialOrd, Ord, PartialEq, Eq)]
struct Entry<K, S>
where
    K: Ord + Eq,
    S: Stream,
{
    key: Reverse<K>,
    #[educe(PartialOrd(ignore), Ord(ignore), PartialEq(ignore))]
    item: S::Item,
    #[educe(PartialOrd(ignore), Ord(ignore), PartialEq(ignore))]
    stream: S,
}

struct PendingEntry<K, S> {
    last_key: K,
    stream: S,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::prelude::*;

    #[async_std::test]
    async fn join_ordered_vecs() {
        {
            let output: Vec<_> = join_ordered_streams(
                |&val| val,
                [
                    stream::iter(vec![1, 3, 3, 3, 5]).fuse(),
                    stream::iter(vec![2, 2, 2, 3, 4, 6]).fuse(),
                ],
            )
            .collect()
            .await;

            assert_eq!(output, vec![1, 2, 2, 2, 3, 3, 3, 3, 4, 5, 6]);
        }

        {
            let output: Vec<_> = join_ordered_streams(
                |&val| val,
                [
                    stream::iter(vec![1, 3, 3, 3, 5]).fuse(),
                    stream::iter(vec![2, 2, 2, 3, 4, 6]).fuse(),
                    stream::iter(vec![0, 1, 2, 3, 4, 8]).fuse(),
                ],
            )
            .collect()
            .await;

            assert_eq!(
                output,
                vec![0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 4, 4, 5, 6, 8]
            );
        }
    }

    #[async_std::test]
    async fn single_vec() {
        let output: Vec<_> =
            join_ordered_streams(|&val| val, [stream::iter(vec![1, 3, 3, 3, 5]).fuse()])
                .collect()
                .await;

        assert_eq!(output, vec![1, 3, 3, 3, 5]);
    }

    #[async_std::test]
    async fn empty_test() {
        {
            use futures::future::Ready;
            use futures::stream::Once;

            let streams: [Once<Ready<usize>>; 0] = [];
            let output: Vec<_> = join_ordered_streams(|&val: &usize| val, streams)
                .collect()
                .await;
            assert_eq!(output, vec![]);
        }

        {
            let output: Vec<_> =
                join_ordered_streams(|&val: &usize| val, [stream::iter(vec![]).fuse()])
                    .collect()
                    .await;

            assert_eq!(output, vec![]);
        }

        {
            let output: Vec<_> = join_ordered_streams(
                |&val: &usize| val,
                [stream::iter(vec![]).fuse(), stream::iter(vec![]).fuse()],
            )
            .collect()
            .await;

            assert_eq!(output, vec![]);
        }

        {
            let output: Vec<_> = join_ordered_streams(
                |&val| val,
                [
                    stream::iter(vec![]).fuse(),
                    stream::iter(vec![2, 2, 2, 3, 4, 6]).fuse(),
                ],
            )
            .collect()
            .await;

            assert_eq!(output, vec![2, 2, 2, 3, 4, 6]);
        }
    }

    #[async_std::test]
    async fn join_random_vecs() {
        let mut rng = rand::thread_rng();

        for _ in 0..100 {
            let n_vecs = rng.gen_range(1..=100);

            let vecs: Vec<_> = (0..n_vecs)
                .map(|_| {
                    let len = rng.gen_range(0..=100);
                    let first = rng.gen_range(0..=10);

                    let vec: Vec<_> = (0..len)
                        .scan((&mut rng, first), |(rng, acc), _| {
                            let out = *acc;
                            *acc += rng.gen_range(0..=3);
                            Some(out)
                        })
                        .collect();
                    vec
                })
                .collect();

            let mut expect: Vec<_> = vecs.iter().flatten().cloned().collect();
            expect.sort_unstable();

            let streams = vecs.into_iter().map(|vec| stream::iter(vec).fuse());
            let output: Vec<_> = join_ordered_streams(|&val| val, streams).collect().await;

            assert_eq!(output, expect);
        }
    }
}
