# go-ratelimiter

核心算法参考：https://github.com/mennanov/limiters

主要做了以下修改：

1. 支持阻塞等待，主要是适用消费 MQ 数据的场景
2. 支持一次性获取多个 token

目前只实现了基于滑动窗口的限流。