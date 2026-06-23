# 📌 Planner Skills — ready-to-fire prompts

Paste any of these into your Copilot session. They're also in the app's **📌 Prompts** tab (with Copy buttons).

---

## ☀️ Start of day
```
☀️ Start my day in the planner.
1. Add a new tab for today's date.
2. Roll over unfinished tasks from yesterday + open backlog items.
3. Ask me for today's meetings, lunch, and the time I want to stop.
4. Build a realistic time-blocked schedule — P1 first, batch quick wins, warn me if I'm overcommitted.
5. Update todo-data.js and open the app.
```

## 🔄 Sync today → tomorrow
```
🔄 I've updated today's statuses (I clicked Export — ingest my todo-data.js from Downloads first).
Then rebuild tomorrow based on what's done vs not:
- Carry over anything unfinished/WIP into tomorrow or the backlog.
- Keep my fixed slots (meetings, lunch, family, study).
- Re-balance so P1 items fit; flag anything that won't.
Update todo-data.js and open the app.
```

## 🌙 End of day / close the day
```
🌙 Close the day.
1. Status updates: <what's done / WIP / blocked>.
2. Mark each task, move unfinished items to tomorrow or backlog.
3. Short summary: what got done vs what slipped, and why.
4. Prep tomorrow's tab with the rolled-over items.
5. Update todo-data.js and open the app.
```

---

## How to open the app
- **Double-click `Daily Planner` on your Desktop** (shortcut created), or
- Bookmark/pin the browser tab the first time it opens.

## How edits sync
- Quick changes (mark done, fix text): do them in the browser — they save to localStorage and survive refresh.
- To make them permanent / let Copilot use them: click **⬇ Export todo-data.js**, then tell Copilot (it reads it from Downloads).
- When Copilot pushes an update from a prompt, it regenerates the file — just refresh the app.
