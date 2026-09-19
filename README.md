# MIT 6.5840 分布式系统实验

这是 MIT 6.5840《Distributed Systems》Spring 2026 实验代码仓库。

## 相关链接

- [课程主页](https://pdos.csail.mit.edu/6.824/index.html)
- [课程信息与安排](https://pdos.csail.mit.edu/6.824/general.html)
- 课程实验 Git 仓库: git://g.csail.mit.edu/6.5840-golabs-2026

## 实验内容

| 实验 | 主题 | 主要代码目录 |
| --- | --- | --- |
| Lab 1 | MapReduce：coordinator、worker 与任务故障处理 | `src/mr`、`src/mrapps` |
| Lab 2 | 线性一致、至多一次语义的键/值服务与锁 | `src/kvsrv1` |
| Lab 3 | Raft 复制状态机协议 | `src/raft1` |
| Lab 4 | 基于 Raft 的容错键/值服务与 RSM | `src/kvraft1` |
| Lab 5 | 分片键/值服务与配置变更 | `src/shardkv1` |

## 环境要求

- Go 1.22 或更高版本
- GNU Make
- Git

检查 Go 版本：

```bash
go version
```

本仓库的 Go module 位于 `src`，module 名称为 `6.5840`。运行实验命令前请进入该目录：

```bash
cd /path/to/6.5840/src
```

## 运行测试

`src/Makefile` 提供了统一的构建和测试入口。默认会使用详细输出和竞态检测：

```bash
# 运行全部实验测试
make all

# 运行单个实验
make mr
make kvsrv1
make raft1
make rsm1
make kvraft1
make shardkv
```

也可以通过 `RUN` 传入 Go 的测试筛选参数：

```bash
# 只运行 Raft 的 3A 测试
make RUN='-run 3A' raft1

# 只运行 MapReduce 的 Wc 测试
make RUN='-run Wc' mr

# 运行 RSM 的 4A 测试
make RUN='-run 4A' rsm1
```

单独运行某个包时，也可以使用标准 Go 命令。例如：

```bash
cd raft1
go test -v -race
```

实验 5 的测试可能运行较久，Makefile 已设置 15 分钟超时：

```bash
cd ..
make shardkv
```

### 使用另一种 Raft 实现

测试命令支持通过 `RAFT` 变量选择状态机版本：

```bash
make RAFT=--raft-state-machine raft1
```

## MapReduce 手动运行

除了测试套件，还可以手动启动 MapReduce。以下命令从 `src` 目录执行：

```bash
cd /path/to/6.5840/src
go build -buildmode=plugin -o mrapps/wc.so mrapps/wc.go
rm -f main/mr-out-*

# 终端 1：启动 coordinator
cd main
go run mrcoordinator.go sock123 pg-*.txt

# 终端 2：启动 worker
go run mrworker.go ../mrapps/wc.so sock123
```

测试套件通常会自动处理构建和临时文件；手动运行时请按照 `lab1.md` 中的参数和工作目录说明操作。

## 目录概览

```text
.
├── lab1.md ... lab5.md   # 实验说明
├── Makefile              # 生成各实验提交压缩包
└── src/
	├── mr/               # MapReduce
	├── kvsrv1/            # 单机 KV 服务与锁
	├── raft1/             # Raft
	├── kvraft1/           # RSM 与 Raft KV 服务
	├── shardkv1/          # 分片 KV 服务
	├── labrpc/ labgob/    # RPC 与序列化支持库
	├── tester1/           # 实验测试框架
	└── main/              # 各实验的启动程序
```

`src/main` 中的可执行文件和测试生成物属于构建产物。实验实现主要位于各实验包目录中，请保留课程提供的接口和启动程序结构，避免修改测试依赖的 RPC、测试框架及启动入口。