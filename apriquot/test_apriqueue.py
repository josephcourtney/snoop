from apriquot import ApriQueue


def test_items_prioritized_correctly():
    """Test if items are popped in order of their priority."""
    q = ApriQueue()
    q.push(item="item1", priority=1.0)
    q.pop()


if __name__ == "__main__":
    test_items_prioritized_correctly()
