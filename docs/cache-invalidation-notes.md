# Cache invalidation implementation notes

The retired-ID FIFO is deliberately not an authorization source or durable deletion log. Its sole purpose is to prevent asynchronous REST results that were already in flight from repopulating presentation timelines after a newer Gateway topology event removed them.

The FIFO remains bounded by the frontend channel-cache budget (with a minimum of eight entries), and active topology events remove matching IDs so thread unarchive transitions continue to work normally.
