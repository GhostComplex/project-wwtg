# 数据源迁移方案：小红书爬虫 → 高德 POI API

**版本：** v1.0
**日期：** 2026-03-26
**状态：** Pending Approval
**决策背景：** 小红书 Playwright 爬虫存在封号风险且无法规模化，团队决议切换至高德 POI API 作为 MVP 数据底座

---

## 一、核心变更

| 项目 | 变更前 | 变更后 |
|------|--------|--------|
| 数据源 | 小红书 Playwright 爬虫 | 高德 POI API |
| 数据类型 | UGC 笔记 → LLM 提取 POI | 结构化 POI 元数据 |
| 推荐理由 | 小红书笔记内容 | LLM 基于 POI 元数据生成 |
| 运行方式 | 需要浏览器 + Cookie | 纯 HTTP API 调用 |
| 封号风险 | 高 | 无 |

**架构不变：** 保持现有预缓存架构 —— `daily_runner` 定时从数据源拉取 POI → 写入 Redis/PostgreSQL → 对话时从缓存读取。对话链路零改动。

---

## 二、数据获取流程

### 2.1 触发方式

保持 `daily_runner` 定时任务（每日凌晨 3:00），仅替换数据源入口。

### 2.2 高德 API 接口

| 接口 | 用途 | 端点 |
|------|------|------|
| 周边搜索 | 按城市中心 + 半径搜 POI | `/v3/place/around` |
| 关键词搜索 | 按关键词 + 城市搜 POI | `/v3/place/text` |

### 2.3 分类覆盖

| 场景 | 高德 types 编码 | 说明 |
|------|----------------|------|
| 景点/公园 | `110000` | 风景名胜 |
| 餐饮 | `050000` | 餐饮服务 |
| 咖啡/下午茶 | `050500` | 咖啡厅 |
| 亲子 | `141200\|141300` | 亲子乐园/儿童乐园 |
| 休闲娱乐 | `080000` | 休闲场所 |
| 博物馆/展览 | `110201` | 博物馆 |

### 2.4 每日调用策略

```
for city in [苏州, 上海, 杭州]:
    for type_code in [110000, 050000, 050500, 141200, 080000, 110201]:
        调用 /v3/place/text
        参数: city={city}, types={type_code}, offset=25, page=1-3
        每个分类拉取 50-75 条 POI
```

每城市 6 个分类 × 3 页 = ~18 次 API 调用
3 个城市 × 18 = **~54 次/天**（远低于免费额度 5000 次/天）

---

## 三、数据结构映射

### 3.1 高德字段 → 内部 POI 模型

| 高德 API 字段 | 内部 POIData 字段 | 处理方式 |
|---------------|-------------------|----------|
| `name` | `name` | 直接映射 |
| `address` | `address` | 直接映射 |
| `location` (lng,lat) | 用于生成 `nav_link` | `MapService.generate_nav_link()` |
| `type` | `tags` | 按映射表转为用户友好标签 |
| `biz_ext.rating` | 新增 `rating` 字段 | 有则取，无则为 null |
| `tel` | 新增 `phone` 字段 | 可选 |
| `pname` + `cityname` + `adname` | `city` | 取 cityname |
| — | `source_type` | 固定为 `"amap"` |
| — | `description` / `reason` | LLM 生成 |
| — | `source_url` / `source_likes` | 不再使用，置为 null |

### 3.2 分类映射表（type → 用户标签）

```python
AMAP_TYPE_MAPPING = {
    "风景名胜": ["景点", "户外"],
    "公园广场": ["公园", "免费", "户外"],
    "植物园": ["赏花", "自然"],
    "动物园": ["亲子", "户外"],
    "博物馆": ["文化", "室内", "免费"],
    "中餐厅": ["美食"],
    "咖啡厅": ["咖啡", "下午茶", "休闲"],
    "亲子乐园": ["亲子", "遛娃"],
    "游乐场": ["亲子", "娱乐"],
    # ... 按需扩展
}
```

---

## 四、LLM 生成推荐理由

### 4.1 触发时机

仅对 `daily_runner` 缓存的 POI 批量生成，**不在对话时实时调用**。

### 4.2 输入

每个 POI 的元数据：地点名称 + 分类 + 评分 + 地址 + 当前季节

### 4.3 输出

| 字段 | 说明 | 示例 |
|------|------|------|
| `tags` | 3-5 个场景标签 | `["孕妇友好", "免费", "花季推荐"]` |
| `reason` | ≤50 字口语化推荐理由 | `"春天玉兰花开满园，平路多适合散步，旁边就是双塔市集可以吃吃逛逛"` |
| `suitable_for` | 适合人群 | `["情侣", "亲子", "独自"]` |
| `cost_range` | 花费区间 | `"免费"` / `"50以内"` |

### 4.4 Prompt 设计

```
你是周末出行推荐助手。根据以下POI信息，生成推荐理由和标签。

POI: {name}
分类: {type}
评分: {rating}
地址: {address}
季节: {season}

输出JSON:
{
  "tags": ["标签1", "标签2", ...],
  "reason": "50字以内的口语化推荐理由",
  "suitable_for": ["适合人群"],
  "cost_range": "花费区间"
}
```

批量处理：每次传 10 个 POI，减少 LLM 调用次数。
3 城 × ~300 POI ÷ 10/batch = **~90 次 LLM 调用/天**

---

## 五、成本估算

| 项目 | MVP 阶段（DAU ≤50） | 增长期（DAU ~500） |
|------|---------------------|---------------------|
| 高德 API | ¥0（~54 次/天，免费额度 5000） | ¥0（~54 次/天，仍在免费额度内） |
| LLM（Qwen/DeepSeek） | ~¥1-2/天（~90 次调用） | ~¥1-2/天（POI 量不随 DAU 增长） |
| **月合计** | **~¥30-60** | **~¥30-60** |

注：LLM 费用仅为 daily_runner 批量生成推荐理由，不随用户量增长。对话中的 LLM 调用（方案生成）费用另计，与数据源无关。

---

## 六、代码改动范围

### 6.1 新增

| 文件 | 说明 |
|------|------|
| `backend/app/services/amap_poi_service.py` | 高德 POI 搜索封装（周边搜索 + 关键词搜索） |
| `backend/app/pipeline/amap_config.py` | 分类编码、城市列表、搜索策略配置 |

### 6.2 修改

| 文件 | 改动 |
|------|------|
| `backend/app/pipeline/daily_runner.py` | 数据源从 XHSCrawler 切换为 AmapPoiService |
| `backend/app/services/data_service.py` | `_crawl_city()` → `_fetch_city_pois()`，调高德 API 替代 Playwright |
| `backend/app/models/schemas.py` | `POIData.source_type` 新增 `"amap"` 枚举值；新增 `rating`, `phone` 可选字段 |
| `backend/app/services/llm_service.py` | 新增 `generate_poi_recommendations()` 方法（基于 POI 元数据生成推荐理由） |

### 6.3 废弃（暂不删除，注释停用）

| 文件 | 说明 |
|------|------|
| `backend/app/services/crawler/` | 整个 crawler 目录停用 |
| `backend/app/pipeline/import_notes.py` | 小红书笔记导入停用 |
| `tools/fetch_note_details.py` | 小红书工具停用 |

### 6.4 PRD 联动改动

| PRD 章节 | 改动 |
|----------|------|
| 主流程后台处理 | 数据源描述更新 |
| 功能清单 | "查看原帖"从 P1 降级到 P2 |
| 方案卡片字段 | 移除小红书链接，改为高德 POI 来源标注 |
| 技术依赖表 | 移除小红书爬虫，新增高德 POI API |
| 风险表 | 移除封号风险，新增 API 限额风险 |

---

## 七、执行计划（2 天）

| 天数 | 任务 | 产出 |
|------|------|------|
| **Day 1** | 高德 POI API 接入 + 字段映射 + daily_runner 改造 + 数据写入 Redis/PG | `amap_poi_service.py` + 改造后的 `daily_runner.py` + `data_service.py` |
| **Day 2** | LLM 推荐理由 prompt 设计 + 联调测试（苏州 3 场景验证） | `llm_service.py` 新增方法 + 端到端测试通过 |

**执行人：** SuperCrew
**前置条件：** Juanjuan 确认方案

---

## 八、风险与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 高德 POI 无 UGC 内容 | 推荐理由质量依赖 LLM | Prompt 精调 + 人工 spot check |
| 高德评分覆盖不全 | 部分 POI 无评分 | 无评分时隐藏评分字段 |
| 免费额度不够（未来） | 超量需付费 | 当前远低于限额；增长时申请企业认证（30万次/天） |
| LLM 生成内容不准确 | 推荐理由可能失真 | 标注为"AI 推荐"，后续接入真实 UGC 替换 |

---

## 九、决策记录

| 日期 | 决策 | 参与人 |
|------|------|--------|
| 2026-03-26 | 停用小红书 Playwright 爬虫，切换至高德 POI API 作为 MVP 数据底座 | Juanjuan, SuperBoss, pm-Octopus, SuperCrew |
| 2026-03-26 | 保持预缓存架构不变，仅替换 daily_runner 数据源入口 | SuperBoss, SuperCrew |
| 2026-03-26 | UGC 内容由 LLM 基于 POI 元数据生成补充 | SuperBoss, SuperCrew, pm-Octopus |
