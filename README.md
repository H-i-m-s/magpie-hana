# Magpie for Hana

把 [magpie](https://github.com/yetone/magpie)（yetone 的多 agent 模型统一管理器）包装成 **HanaAgent 的 v2 App**。

## 它解决什么问题

magpie 是个很好用的东西：一个界面统一管住机器上所有 AI agent 的模型，还能把你在某处登录过的订阅复用给别的工具。但它有个使用上的摩擦——**你得记得打开它**。托盘图标常驻，每次换模型都要先找到那个图标。

这个 App 把 magpie 变成 Hana 的一部分：

- **exe 随包携带**，装完即用，不用去下载、不用记得它在哪
- **Hana 启动即拉起 magpie**（无托盘、无窗口），**Hana 关闭即随之停止** —— 进程的开关握在 Hana 手里
- **整页工作区**：在 Hana 里直接用 magpie 的完整界面，配色跟随 Hana 主题
- **magpie 用不上的功能可以隐藏**（只藏入口，不解除底层能力）
- 另有一个 `magpie` 工具，让 Agent 能查模型清单、订阅额度、按来源分组看可用模型

## 安装

从 Hana 的扩展市场安装，或使用 `dist/` 里的打包产物。

首次加载会请求以下能力，都可在 设置 → 应用能力 里随时撤销：

| 能力 | 用途 |
|---|---|
| `app/runtime.execute` | 拉起 magpie 本体 |
| `app/runtime.local-machine` | 以本机用户权限运行它 |
| `app/runtime.network` | 它要连外网调模型，也要监听本地端口 |
| `app/tools.expose-to-model` | 让 Agent 能调用 `magpie` 工具 |

## 它放在哪、会不会动我的东西

**不会动你的任何现有配置。** 具体来说：

```
{HANA_HOME}/apps/magpie-hana/         插件本体（只读）
  vendor/magpie-windows-amd64.exe     内置的 exe
{HANA_HOME}/app-data/magpie-hana/     运行时数据（可写、卸载保留）
  bin/magpie.exe                      从 vendor 播种来的
  bin/data/                           便携模式：magpie 的全部状态都在这
```

- **不碰** `~/.config/magpie`（你自己装的 magpie 的配置目录）
- **不抢** 3425 端口
- **不自动更新** exe（更新会重启进程、中断对话，所以交给你手动决定）
- exe 只在**缺失时**从包内播种，**已存在绝不覆盖**（避免降级事故）

## 界面

工作区挂载在 magpie 自己的 web 界面上（`magpie web` 模式），由本 App 的反代注入：

- **主题**：把 Hana 当前的明暗与配色映射成 magpie 的 CSS 变量
- **功能隐藏**：设置页里可以勾选隐藏 Library / Sessions / Routing 等入口

隐藏是「盖住」而不是「删掉」：底层能力还在，只是界面上没有入口。如果哪天 magpie 改了选择器名，表现是**那个入口重新冒出来**，不会白屏。

## 工具

```
magpie(action="status")        托管状态、端口、网关健康
magpie(action="models")        列出全部可用模型
magpie(action="quotas")        各订阅的额度余量
magpie(action="agents")        按来源分组看可用模型
magpie(action="hidden", hidden=["library","sessions"])   设置隐藏
magpie(action="start"|"stop")  手动起停
```

## 已知边界

- **仅 Windows**。exe 只带了 Windows 版；市场包 50 MiB 的限制装不下多平台的 exe。
- **更新需手动**：magpie 会自更新，但 `magpie web` 模式本身不含自动更新逻辑；本 App 也不自动执行，因为更新会重启进程。
- **首次启动要等几秒**：工作区会显示「magpie 正在启动…」，就绪后自动切换。

## 开发

```
node tools/selftest.mjs <exe路径> <隔离工作目录>     # 隔离自测：不碰真实环境
```

设计文档、全部已核实事实与红线清单见 `doc/接手文档.md`。

## 许可证

本 App 以 MIT 授权，见 `LICENSE`。

内置的 magpie 由 [yetone](https://github.com/yetone) 开发，同样以 MIT 授权，
版权归 Copyright (c) 2026 yetone 所有。许可证原件见 `vendor/LICENSE-magpie`。
本 App 未修改 magpie 的任何代码，只是通过网络接口与命令行调用它。
