# Design

<!-- impeccable:design-schema 1 -->

## Direction & Thesis

- **Direction**: Clean Minimalist File Manager (Operate mode)
- **Thesis**: A distraction-free, high-density P2P file manager that replaces bloated generic admin templates with a crisp, tactile node control center featuring live chunk-state ribbons, distributed mesh search, and peer topology drawers.

## Palette & Theming

Dual-theme color system using CSS Custom Properties:

| Token | Dark Mode (`[data-theme="dark"]`) | Light Mode (`[data-theme="light"]`) | Usage |
|---|---|---|---|
| `--bg-ground` | `#0B0F17` | `#F8FAFC` | Page canvas background |
| `--bg-surface` | `#131B2A` | `#FFFFFF` | Primary cards and panels |
| `--bg-surface-elevated`| `#1A2438` | `#F1F5F9` | Secondary cards, dropzone, table headers |
| `--bg-surface-hover` | `#1F2C44` | `#E2E8F0` | Interactive hover states |
| `--bg-terminal` | `#070A10` | `#0F172A` | Diagnostic output console |
| `--border-subtle` | `#1E293B` | `#E2E8F0` | Hairline card/table borders |
| `--border-strong` | `#334155` | `#CBD5E1` | Input outlines and delimiters |
| `--text-primary` | `#F8FAFC` (17.5:1) | `#0F172A` (17.8:1) | Headings, file names, primary metrics |
| `--text-secondary` | `#CBD5E1` (10.4:1) | `#334155` (10.1:1) | Labels, status text, metadata |
| `--text-muted` | `#94A3B8` (6.1:1) | `#64748B` (4.98:1) | Secondary captions, timestamps, shortcuts |
| `--accent-primary` | `#6366F1` | `#4F46E5` | Primary action buttons, brand icons |
| `--status-success` | `#10B981` | `#059669` | Completed chunks, online health status |
| `--status-warning` | `#F59E0B` | `#D97706` | Pending transfers, warnings |
| `--status-info` | `#0284C7` | `#0284C7` | Downloading chunks, active streaming |
| `--status-error` | `#EF4444` | `#DC2626` | Failed chunks, disconnected alerts |

## Typography

- **System Stack**: `-apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", Roboto, Helvetica, Arial, sans-serif`
- **Monospace Stack**: `"SFMono-Regular", Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace`
- **Tabular Figures**: `font-variant-numeric: tabular-nums;` on all byte sizes, metrics, ports, and chunk indices.

## Surface Floor Checks

- **Contrast**: Guaranteed ≥ 4.5:1 across all themes and text levels.
- **Refused**: Zero kickers/eyebrows, zero gradient text, zero fake glassmorphism blur, zero nested cards, zero emoji icons.
- **Icons**: 100% authored inline geometric SVG icons with 1.75px/2px stroke, crisp vector curves, and unified bounding boxes.
- **Micro-Interactions**:
  - Live chunk pulse animation (`@keyframes chunkPulse`) during active downloads.
  - Split-flap chunk blocks with retry count chips on failure.
  - Copy-to-clipboard checkmark animations.
  - Polling pulse indicator and spinner animations during sync.
  - Floating auto-dismissing toast notifications.
