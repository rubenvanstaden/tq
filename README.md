# Distributed Task Queue

Task Queue in Go using Redis Streams

Relationship between task name and stream naming:

```
[task]:[stream]

# Example
upload:default
upload:result
```

No, cause we want to queue to have hetereogious task types.
