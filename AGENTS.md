# Repository Agent Instructions (`zy-quant`)

Before planning tasks, creating branches, writing strategy/execution/deploy code, reviewing diffs, or merging in this repository, **always read and strictly follow [`SKILL.md`](SKILL.md)** and check the active roadmap in [`TODO.md`](TODO.md) (archived milestones live in [`HISTORY.md`](HISTORY.md)).

Key non-negotiable rules enforced by `SKILL.md`:
- Public GitHub identity only (`zyang21` / `zyang21@users.noreply.github.com`, `commit.gpgsign=false`); zero secrets or runtime DBs in tracked files.
- English only (no Chinese characters) in all shell scripts (`deploy/*.sh`), shell comments, and terminal commands.
- Strict causality (`bisect_left` on `1d` bars, poison-bar regression tests), Futu HK `GTC` stop & fractional-share boundaries, `America/New_York` (`ET_TZ`) date anchoring, and Dual-Agent (`Dev Agent` + fresh-context read-only `Review Agent`) audit before merging features to `main`.
