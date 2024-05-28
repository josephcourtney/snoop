import contextlib
import datetime
import threading
import time

import pytest
from apriquot import ApriQueue, ItemExpiredError, QueueEmptyError, RetryLimitExceededError


def test_add_item_to_priority_heap():
    """Verify that an item can be added to the queue and is correctly stored in the priority heap."""
    q = ApriQueue()
    item_id = q.push(item="test_item", priority=1.0)
    assert item_id is not None


def test_items_prioritized_correctly():
    """Test if items are popped in order of their priority."""
    q = ApriQueue()
    item_id1 = q.push(item="item1", priority=1.0)
    item_id2 = q.push(item="item2", priority=0.5)
    item_id3 = q.push(item="item3", priority=2.0)
    item = q.pop()
    assert item.id == item_id3  # Highest priority first

    item = q.pop()
    assert item.id == item_id1  # Next highest priority

    item = q.pop()
    assert item.id == item_id2  # Lowest priority


def test_no_mature_items():
    """Verify that attempting to pop an item before any items have matured results in a QueueEmptyError."""
    q = ApriQueue()
    q.push(
        item="test_item",
        priority=1.0,
        matures=datetime.datetime.now(tz=datetime.UTC) + datetime.timedelta(days=1),
    )
    with pytest.raises(QueueEmptyError, match="No eligible items to pop at the current time"):
        q.pop()


def test_matures_after_delay():
    """Ensure that tasks respect their maturation times and are not popped before they mature."""
    q = ApriQueue()
    item_id = q.push(
        item="test_item",
        priority=1.0,
        matures=datetime.datetime.now(tz=datetime.UTC) + datetime.timedelta(seconds=0.5),
    )
    with pytest.raises(QueueEmptyError, match="No eligible items to pop at the current time"):
        q.pop()
    time.sleep(1)
    item = q.pop()
    assert item.id == item_id


def test_valid_before_deadline():
    """Ensure that items can be popped before their deadline expires."""
    q = ApriQueue()
    item_id = q.push(
        item="test_item",
        priority=1.0,
        deadline=datetime.datetime.now(tz=datetime.UTC) + datetime.timedelta(days=1),
    )
    item = q.pop()
    assert item.id == item_id


def test_expires_after_delay():
    """Verify that items expire correctly and are not available to pop after their deadline has passed."""
    q = ApriQueue()
    q.push(
        item="test_item",
        priority=1.0,
        deadline=datetime.datetime.now(tz=datetime.UTC) + datetime.timedelta(seconds=0.5),
    )
    time.sleep(1)
    with pytest.raises(QueueEmptyError, match="No eligible items to pop at the current time"):
        q.pop()


def test_push_rejects_expired_item():
    """Ensure that the queue rejects items that are already expired at the time of pushing."""
    q = ApriQueue()
    with pytest.raises(ItemExpiredError, match="Item .* has already expired"):
        q.push(
            item="test_item",
            priority=1.0,
            deadline=datetime.datetime.now(tz=datetime.UTC) - datetime.timedelta(seconds=1.0),
        )


def test_push_rejects_item_that_expires_before_maturation():
    """Verify that the queue rejects items with a deadline earlier than their maturation time."""
    q = ApriQueue()
    with pytest.raises(ValueError, match="Item .* expires before maturation"):
        q.push(
            item="test_item",
            priority=1.0,
            matures=datetime.datetime.now(tz=datetime.UTC) + datetime.timedelta(seconds=1.0),
            deadline=datetime.datetime.now(tz=datetime.UTC) + datetime.timedelta(seconds=0.5),
        )


def test_valid_in_window():
    """Check that items can be popped between their maturation time and deadline."""
    q = ApriQueue()
    test_id = q.push(
        item="test_item",
        priority=1.0,
        matures=datetime.datetime.now(tz=datetime.UTC) + datetime.timedelta(seconds=0.1),
        deadline=datetime.datetime.now(tz=datetime.UTC) + datetime.timedelta(seconds=10.0),
    )
    time.sleep(0.2)
    item = q.pop()
    assert item.id == test_id


def test_item_requeue_with_backoff():
    """Test that failed items are requeued with a backoff mechanism and the retry count is incremented."""
    q = ApriQueue()
    q.push(
        item="test_item",
        priority=1.0,
    )
    item = q.pop()
    q.retry_item(item.id)
    for _ in range(30):
        with contextlib.suppress(QueueEmptyError):
            item = q.pop()
            time.sleep(0.1)
    assert item.retries == 1


def test_max_retries():
    """Verify retry limit is respected and raise a RetryLimitExceededError after the maximum retries."""
    q = ApriQueue()
    item_id = q.push(
        item="test_item",
        priority=1.0,
    )
    item = None
    for _ in range(100):
        with contextlib.suppress(QueueEmptyError):
            item = q.pop()
            break
        time.sleep(0.01)
    assert item is not None
    q.retry_item(item.id)

    for _ in range(100):
        with contextlib.suppress(QueueEmptyError):
            item = q.pop()
            break
        time.sleep(0.01)
    assert item is not None
    q.retry_item(item.id)

    for _ in range(200):
        with contextlib.suppress(QueueEmptyError):
            item = q.pop()
            break
        time.sleep(0.01)
    assert item is not None
    q.retry_item(item.id)

    for _ in range(200):
        with contextlib.suppress(QueueEmptyError):
            item = q.pop()
            break
        time.sleep(0.01)
    assert item is not None
    with pytest.raises(RetryLimitExceededError, match="No more retries left for item .*"):
        q.retry_item(item.id)
    assert q.item_map[item_id].retries == 4


def test_group_token_bucket():
    """Check if the rate limiting is enforced, ensuring items are processed at the correct rate."""
    q = ApriQueue()
    group_id = q.new_group(name="test_group", max_tokens=2, refill_rate=1)

    item_id1 = q.push(item="test_item_1", priority=1.0, group=group_id)
    item_id2 = q.push(item="test_item_2", priority=1.0, group=group_id)
    item_id3 = q.push(item="test_item_3", priority=1.0, group=group_id)

    item1 = q.pop()
    assert item1.id == item_id1

    item2 = q.pop()
    assert item2.id == item_id2

    with pytest.raises(QueueEmptyError, match="No eligible items to pop at the current time"):
        q.pop()

    time.sleep(1)  # Allow time for token refill

    item3 = q.pop()
    assert item3.id == item_id3


# def test_concurrent_access():
#     """Verify that items can be added and popped concurrently without issues."""
#     q = ApriQueue()
#
#     def add_items():
#         for i in range(100):
#             q.push(item=f"item_{i}", priority=i)
#
#     def pop_items():
#         popped_items = []
#         for _ in range(100):
#             with contextlib.suppress(QueueEmptyError):
#                 item = q.pop()
#                 popped_items.append(item)
#         return popped_items
#
#     add_thread = threading.Thread(target=add_items)
#     pop_thread = threading.Thread(target=pop_items)
#
#     add_thread.start()
#     pop_thread.start()
#
#     add_thread.join()
#     pop_thread.join()
#
#     assert len(q.priority_heap) == 0


def test_high_load_performance():
    """Check the performance of the queue under high load by adding and popping a large number of items."""
    q = ApriQueue()

    for i in range(10000):
        q.push(item=f"item_{i}", priority=i)

    start_time = time.time()
    for _ in range(10000):
        q.pop()
    end_time = time.time()

    assert end_time - start_time < 5  # Ensure it completes within a reasonable time


def test_identical_priorities():
    """Verify that items with identical priorities are handled correctly and all items are processed."""
    q = ApriQueue()
    item_id1 = q.push(item="item1", priority=1.0)
    item_id2 = q.push(item="item2", priority=1.0)
    item_id3 = q.push(item="item3", priority=1.0)

    popped_items = [q.pop().id for _ in range(3)]
    assert set(popped_items) == {item_id1, item_id2, item_id3}


def test_immediate_requeue_after_failure():
    """Ensure that items can be immediately requeued after failure with zero backoff."""
    q = ApriQueue()
    item_id = q.push(item="test_item", priority=1.0)
    item = q.pop()
    q.retry_item(item.id, backoff=0)
    retried_item = q.pop()
    assert retried_item.id == item_id
    assert retried_item.retries == 1


def test_no_maturation_or_expiration():
    """Verify that items without maturation or expiration times are handled correctly."""
    q = ApriQueue()
    item_id = q.push(item="test_item", priority=1.0)
    item = q.pop()
    assert item.id == item_id


def test_past_maturation_time():
    """Ensure that items with a past maturation time are immediately available."""
    q = ApriQueue()
    item_id = q.push(
        item="test_item",
        priority=1.0,
        matures=datetime.datetime.now(tz=datetime.UTC) - datetime.timedelta(seconds=1),
    )
    item = q.pop()
    assert item.id == item_id


def test_high_frequency_token_consumption():
    """Test high-frequency token consumption scenarios to ensure correct item processing order."""
    q = ApriQueue()
    group_id = q.new_group(name="test_group", max_tokens=5, refill_rate=5)

    for i in range(10):
        q.push(item=f"item_{i}", priority=1.0, group=group_id)

    time.sleep(1)  # Allow time for token refill

    for _ in range(5):
        item = q.pop()
        assert item.group == group_id


def test_invalid_item():
    """Ensure that invalid items (e.g., with invalid priority) raise appropriate exceptions."""
    q = ApriQueue()
    with pytest.raises(ValueError, match="Priority must be a non-negative integer"):
        q.push(item="invalid_item", priority="invalid_priority")
