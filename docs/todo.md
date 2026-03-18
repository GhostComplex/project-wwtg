# 项目 TODO — 周末搭子 MVP

**最后更新：** 2026-03-19

---

## 本周（3/17 - 3/21）

### Dev — SuperCrew002
- [x] 技术方案文档更新（成本 < 300/月 + Azure 部署 + review 反馈）
- [ ] LLM Benchmark — DeepSeek V3 vs Qwen-Max（延迟/质量/成本）
- [ ] 小红书数据采集方案调研（生产级方案，参考但不复用 POC skill）

### PM — pm-Octopus
- [x] PRD v1.1 完成并推到 repo
- [x] 爬虫 skill 技术细节同步
- [ ] 跟进 Juanjuan 确认 MVP 里程碑节奏（4 周 or 6 周）

### 需要 Juanjuan 推进
- [ ] 微信小程序认证（W3 内测前必须完成）
- [ ] Azure 资源开通（部署时需要）
- [ ] 高德地图 API Key 注册申请

---

## W1 启动后

### Dev
- [x] 项目脚手架搭建（FastAPI 后端 + 微信小程序骨架）
- [x] 数据管线 POC（自研爬虫 + Redis 缓存跑通）
- [x] 对话状态机 + 意图解析基础实现

### PM
- [ ] 准备内测用户名单（团队 + 10 个种子用户）
- [ ] 内测反馈收集方案

---

## W2

- [x] 方案生成核心逻辑 + 天气/地图 API 接入
- [x] 小程序卡片 UI + 同步 JSON + loading 动画展示（MVP 不做 SSE，per M3 决策）
- [x] 端到端可用验证

## W3

- [x] 方案详情页 + 拒绝重推 + 埋点接入
- [ ] 内测部署 + 团队试用
- [ ] 收集反馈

## W4

- [ ] Bug 修复 + 性能优化（15 秒目标）
- [ ] 小程序审核 + 正式发布

---

## Completed Milestones (M6-M11)

| Milestone | Summary |
|-----------|---------|
| M6 | Project scaffold: FastAPI backend + mini program skeleton, conversation state machine |
| M7 | Data pipeline POC: Playwright scraper + Redis cache, intent extraction |
| M8 | Plan generation: LLM orchestrator, weather/map API integration, 2-plan output |
| M9 | Mini program card UI: plan cards, action buttons, sync JSON + loading animation |
| M10 | Plan detail page, reject/re-push flow, analytics event tracking |
| M11 | Bug fix (reject API call), CI/CD pipeline, documentation update |

---

## 决策待定

| 问题 | 状态 | 负责人 |
|------|------|--------|
| MVP 节奏 4 周 or 6 周 | 待 Juanjuan 确认 | Juanjuan |
| LLM 最终选型 | 待 benchmark 结果 | SuperCrew002 |
| 云服务 Azure 具体方案 | 待部署时确定 | SuperCrew002 + Juanjuan |
| SSE streaming | 决策为 MVP 不做，用同步 JSON + loading 动画 | 已决定 | M3 review |
