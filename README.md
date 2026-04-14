# Liepin Job Board

静态职位看板，聚焦猎聘公开页面中的内容安全、风控、风险运营、风险评估等相关岗位。

## 功能

- 每日抓取猎聘公开 SEO 职位页
- 输出当天累计新增、当前全量快照、Markdown 摘要
- 维护 SQLite 历史库，保留每日快照与运行趋势
- 生成 `docs/` 静态站点，可直接发布到 GitHub Pages

## 本地运行

```bash
python3 -m pip install -r requirements.txt
python3 liepin_daily_jobs.py
```

站点入口：

- `docs/index.html`

## 自动更新

仓库内置 GitHub Actions 工作流：

- 每天北京时间 `09:20` 自动运行
- 运行后自动提交最新 `docs/` 和 `liepin_output/`

## 说明

- 仅抓取公开页面，不处理登录、验证码或任何反爬绕过
- 结果依赖猎聘公开页结构，若页面改版需要同步调整解析逻辑
