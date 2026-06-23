# 📌 Planner Skills — ready-to-fire prompts

These are the Daily Planner''s "skills". They''re shown in the app''s **📌 Prompts** tab (with Copy buttons) and mirrored here.

## 🧭 How to use
1. Click **📋 Copy** on a routine (in the app''s Prompts tab) or copy a block below.
2. Paste it into your **Copilot CLI** session and send.
3. Copilot reads and rewrites `todo-data.js`, then the app reloads with the new plan.

Pick by time of day: **☀️ Start of day** (morning) · **🔄 Rebalance** (midday) · **🌙 End of day** (evening) · **📅 Weekly review** (Fri/Sun).
Every prompt follows the same shape — **Read first → Inputs → Steps → Rules → Output** — and asks you for the inputs it needs (meetings, hard stop, energy, top outcome). This matches the SKILL.md template used across the skills library.

Data file: `C:/Users/orenhorowitz/desktop-automations/daily-planner/todo-data.js` (`window.TODO_DATA`, keyed by `YYYY-MM-DD`). The app auto-saves; daily backups land in `history/`.

---

## ☀️ Start of day
```
☀️ START OF DAY — build today''s time-blocked plan.

Read first: the planner data at C:/Users/orenhorowitz/desktop-automations/daily-planner/todo-data.js (window.TODO_DATA, keyed by YYYY-MM-DD) plus the backlog.
Ask me for: today''s meetings (with times), lunch, hard-stop time, my energy level (1-5), and the single most important outcome for today.

Do:
1. Create today''s tab if it does not exist.
2. Roll over unfinished/WIP tasks from the last open day and any due backlog items. Tag each with how many days it has aged; if aged over 3 days, flag it to escalate, shrink, delegate, or drop.
3. Place fixed slots first (meetings + 10-15 min buffers, lunch, family/study), then fill the rest.
4. Time-block the work: P1 in a protected deep-work block during my peak-energy window; batch P2/P3 into one or two admin blocks; group same-context tasks to cut switching.
5. Right-size: sum estimates vs available hours. If over capacity, say so and propose exactly what to cut or defer — do not silently overfill.
6. Add a 10-15 min end-of-day review block.

Rules (every time): P1 before anything else; max 2-3 deep-work blocks; buffers around meetings; never schedule past my hard stop; keep one slack block for surprises.

Output: write the schedule to todo-data.js (the app auto-reloads) and give me a 3-line plan of attack — the one must-win task, the biggest risk, and the first action to take now.
```

## 🔄 Rebalance (midday)
```
🔄 REBALANCE — re-plan the rest of today from current status.

Read first: the latest todo-data.js (the app auto-saves, so statuses are current).

Do:
1. Diff every task: done / WIP / pending / blocked, and note what actually got done vs planned.
2. For each unfinished item decide by priority and aging: keep in a remaining slot today, push to tomorrow, or send to backlog.
3. Preserve all fixed slots (meetings, lunch, family, study).
4. Re-balance so every P1 still fits; if a P1 cannot fit, say so and propose what to drop or defer to make room.
5. For each blocked task, name the blocker and the smallest next action to clear it.

Rules: protect P1; never move a fixed slot; surface overcommit instead of hiding it; keep a slack block.

Output: updated todo-data.js plus a 2-line note on what changed and why.
```

## 🌙 End of day
```
🌙 END OF DAY — close out today and seed tomorrow.

Read first: the latest todo-data.js.
Tell me (or infer from statuses): what is done / WIP / blocked.

Do:
1. Set each task''s final status.
2. Move unfinished items to tomorrow or the backlog by priority and aging; flag anything aged over 3 days.
3. Write a short day summary: what got done, what slipped and why, and one improvement for tomorrow.
4. Pre-build tomorrow''s tab: fixed slots plus rolled-over P1s in the peak-energy window, leaving room for new work.
5. Capture any new tasks or ideas I mention into the backlog with a priority.

Rules: be honest about slippage and name the cause; do not overfill tomorrow; one improvement, not ten.

Output: updated todo-data.js (today closed, tomorrow seeded) plus the day summary.
```

## 📅 Weekly review
```
📅 WEEKLY REVIEW — groom the backlog and set next week''s focus.

Read first: todo-data.js (this week''s day tabs + backlog) and the history/ backups for the week.

Do:
1. Summarize the week: completed vs planned per day, recurring slippage patterns, and time sinks.
2. Groom the backlog: merge duplicates, delete stale items, re-prioritize (P1/P2/P3), and split anything too big to start.
3. Choose next week''s 3 must-win outcomes and map each to specific days.
4. Flag risks: overcommitted days, dependencies, and anything aging badly.

Rules: at most 3 weekly must-wins; prefer deleting over hoarding; protect deep-work time.

Output: a groomed backlog and a one-screen next-week focus written into the planner, plus a short retro (keep / drop / try).
```