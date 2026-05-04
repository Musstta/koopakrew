# Koopa Krew — Development Backlog

## How to use
Mark items `[x]` when done. Add new items under the appropriate section.

---

## 🔴 Critical (dark mode failures)

- [x] Radar chart nearly invisible in dark mode — fill/border/label colors fixed
- [x] Leaderboard risk/hunter rows fail contrast in dark mode — text color override added
- [x] Archive Highlights box olive-green dark mode — replaced with amber accent border

---

## 🟡 Polish & Readability

- [x] Background pattern too loud — opacity reduced (light: 0.28, dark: 0.06)
- [x] "Default" track state chip invisible in dark mode — border + text brightened
- [x] "Locked" state chip should feel gold/amber — updated
- [x] "At Risk" state chip should pulse — animation added
- [x] Active nav page has no visual indicator — left-border accent added; fixed endpoint name (blueprint registers as `main.index`, not `index`)
- [x] Podium cards all same size — rank-1/2/3 now have different heights + permanent medal borders
- [x] CSS changes not loading in browser — added MD5 content-hash cache-busting (`?v=...`) to CSS link; recomputed on every server restart
- [x] Radar chart always rendered in light mode — `isDark` was captured before theme JS ran; fixed by reading `localStorage` directly
- [ ] Events page timestamps low contrast in dark mode — bump to #94a3b8
- [ ] "No racer selected" warning too prominent for spectators — soften or collapse
- [ ] "Profile →" button on spotlight cards should look like a button
- [ ] Player spotlight cards need a color accent to differentiate players visually
- [ ] Highlights grid items all look the same — top items need featured/hero treatment
- [ ] Leaderboard active tab state not dramatic enough — already has strong active style, revisit

---

## 🟢 Layout Improvements

- [ ] Standings page: add hero strip with season name + leading player + race count
- [ ] Events page: timeline layout (vertical line, initials badges, pill state transitions)
- [ ] Sidebar: convert to narrower icon+label or top nav on desktop
- [ ] Stats page: consolidate leaderboard rows into accordion groups per tab

---

## 🚀 Visionary (future)

- [ ] **War Board**: standings as 96-track territory map, each cell color-coded by owner, at-risk pulsing red
- [ ] **Race Night mode**: full-screen live dashboard for session nights — real-time wins, ownership changes, event feed
- [ ] **Head-to-Head**: full rivalry page — Sergio vs Fabian — direct duel count, win rate, contested tracks, timeline
- [ ] **Season Recap**: Spotify Wrapped-style animated cards at season end ("142 wins, 39 tracks, most brutal moment...")
- [ ] **Player Trading Card**: radar chart + stats + AI flavor text as a shareable image card
- [ ] **Cup Sweep progress bar**: "Sergio is 3/4 in Mushroom Cup — 1 track from a sweep" on standings
