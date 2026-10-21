# AGENTS.md

> 本文件是 Claude Code 唯一自动加载的项目契约（`CLAUDE.md` 软链到此）。**核心准则写在本文件**；细化规范拆到 `docs/agents-dot-md/*`、经验沉淀放 `docs/memory/*`（都不自动加载，按需查阅）。下方《项目 Skill 索引》《模块文档索引》《记忆索引》由 agents-dot-md skill 的 `reindex.py` 生成，勿手工编辑标记之间的内容。

## 项目概要
个人常驻 Agent 平台：Go Runtime + Dapr（Workflow / Actor / State / Pub/Sub，后端 Redis）+ k3s；Pi、Codex 等执行器作为 A2A Worker 接入，大脑角色可由任意执行器担任，多 Agent 协作推进 Goal / Loop。
**`docs/spec.md` 是唯一的设计契约**：改接口、状态键、调度语义、决策格式前先改 spec，再改代码；spec 里标「实现前核验」的条目，核验后把结论写回 spec。实施顺序与每步验收见 `docs/plan.md`。

## 工作区规则
- 作用范围：本规则适用于当前仓库及所有子目录。
- 交流语言：默认使用简体中文；仅在用户明确要求时使用英文（详见 `docs/agents-dot-md/translation.md`）。
- 修改原则：仅做最小、精确改动，避免无关重构。
- 安全原则：未经明确授权，不执行破坏性 git / 文件操作。
- 验证要求：代码改动后，尽量执行对应模块级编译或测试验证。
- 自检要求：代码改动后必须逐条对照 `docs/agents-dot-md/code-checklist.md`，全部符合才算完成。

## 编码行为准则（Karpathy 防错指南）
1. **先想后写**：不臆测、不藏困惑；多种解读先摆选项让用户拍板；有更简单实现就直说。
2. **简单优先**：只写当前所需最小代码，不提前抽象、不加没要求的防御。
3. **外科手术式改动**：只动该动的；不顺手重构没坏的；只清理本次改动产生的孤儿。
4. **目标驱动**：把任务转成可验证目标（先写复现 / 非法输入测试再改），多步任务先给带验证点的计划。

> 可执行细则与「改动落地规则」见 `docs/agents-dot-md/coding-guidelines.md`。

## 代码 checklist（强制）
> ⛔ 每次写完 / 改完代码，**必须逐条对照 `docs/agents-dot-md/code-checklist.md` 自检**，全部符合才算完成；不符合的先改到符合，不要留给评审或线上发现。新增条目直接追加到该文件。

## 架构与技术栈
> 动结构 / 加模块 / 接外部依赖前读 `docs/agents-dot-md/architecture.md`（组件边界、分层、关键链路、设计决策）；
> 写代码前读并遵循 `docs/agents-dot-md/tech-stack.md`（技术选型、数据访问与配置约定、构建与验证命令）。与本文件冲突时以本文件为准。

## 技能整理（Skill 维护）
本仓库自带的项目 skill 放在仓库任意位置的 `skills/<name>/SKILL.md`（含各业务模块子目录下的 `skills/`；YAML frontmatter 至少含 `name` / `description`，可带脚本 / 资源同目录）。它们随仓库分发、对所有克隆生效；全局 skill（`~/.claude/skills/*`）不入库，本索引不收录。

- 新增 / 改名 / 删除 skill，或改了 `SKILL.md` 的 `description` 后，在仓库根运行 `python3 ~/.claude/skills/agents-dot-md/scripts/reindex.py .`（Windows 用 `python`）重建下方《项目 Skill 索引》《模块文档索引》与 `docs/agents-dot-md/00-index.md`。
- **不要手工编辑** `<!-- SKILLS:START -->…<!-- SKILLS:END -->` 与 `<!-- MODULES:START -->…<!-- MODULES:END -->` 之间的内容——会被脚本覆盖。
- `description` 写清「何时用 / 触发词」，首句作为索引摘要（脚本取首句）；触发要精准，避免与既有 skill 语义重叠。

## 记忆记录（Memory）
本仓库的记忆区是 `docs/memory/*.md`——**纯 Markdown，不依赖外部记忆服务，也不需要 LLM key**，你自己用读写文件的工具维护，检索时直接 `grep` / 读文件。

- **何时记**：完成一次排查 / 根因分析 / 踩坑修复后，把「非显然、下次能省事」的结论写下来，别让下一个会话重新推导。
- **记在哪**：按主题聚合到一个文件（如 `docs/memory/build-and-deploy.md`、`docs/memory/known-pitfalls.md`、`docs/memory/external-integrations.md`），不要一条一个文件。文件头两行必须是 `# 标题` 与 `> 一句话摘要`（否则进不了索引）。
- **和记忆服务的分工**：本环境另有记忆服务（如 mem0）时，只和本仓库有关的结论写这里，随 git 共享给团队和其他机器；跨项目的个人偏好和本机环境事实（代理、凭据放在哪、本机工具版本）写记忆服务，没有记忆服务就不记，不要写进仓库。同一条事实只写一处。
- **每条怎么写**：一行一条，**绝对日期**打头，写清「现象 → 原因 → 结论 / 做法」；能附证据就附（`文件:行`、命令、报错原文）。未验证的猜测标「待验证」。
- **不记什么**：代码结构、git 历史、本文件或模块里已写过的内容；凭据 / 密钥只落未入库的本地文件（如 `dev-env.local.md`），**绝不**写进 `docs/memory/` 或任何入库文件。
- 增删主题文件后在仓库根运行 `python3 ~/.claude/skills/agents-dot-md/scripts/reindex.py .` 重建下方《记忆索引》。与某个任务关联的结论，可按需另记到任务系统（如 `vikunja`）的评论里。

## 📇 项目 Skill 索引（全仓 SKILL.md，脚本生成）
<!-- SKILLS:START -->
- （仓库内暂无 SKILL.md）
<!-- SKILLS:END -->

## 📂 模块文档索引（docs/agents-dot-md/，脚本生成）
<!-- MODULES:START -->
- [系统架构](docs/agents-dot-md/architecture.md) — 组件边界、依赖方向、关键链路与设计决策；动结构、加模块、接外部依赖前先看本文。完整契约以 `docs/spec.md` 为准。
- [代码 checklist（强制自检）](docs/agents-dot-md/code-checklist.md) — 写完 / 改完代码必须逐条对照的硬性自检清单；新增条目直接追加本文件。
- [编码行为准则（Karpathy 防错指南 · 展开）](docs/agents-dot-md/coding-guidelines.md) — 写 / 改 / 审代码的可执行细则；AGENTS.md 顶层只留四条高压线，细则在此。
- [开发环境 / 推送 / 发布](docs/agents-dot-md/environment.md) — 凭据存放约定、镜像源与代理、k3s 部署操作；动环境或部署前先看本文。
- [技术栈与实现规范](docs/agents-dot-md/tech-stack.md) — 技术选型、存储与配置约定、构建与验证命令；写代码前先看本文，按既有方式扩展。
- [交流与语言约定](docs/agents-dot-md/translation.md) — 默认简体中文交流与注释；仅用户明确要求时用英文。
<!-- MODULES:END -->

## 🧠 记忆索引（docs/memory/，脚本生成）
<!-- MEMORY:START -->
- [M0 开发决策与排查记录](docs/memory/m0-development-decisions.md) — 记录 M0 阶段关键架构决策、踩坑与修复结论，避免后续会话重新推导。
<!-- MEMORY:END -->
