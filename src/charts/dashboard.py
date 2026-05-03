from matplotlib.figure import Figure

_PALETTE = [
    "#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f",
    "#edc948", "#b07aa1", "#ff9da7", "#9c755f", "#bab0ac",
]
_TEXT_COLOR = "#cccccc"
_MUTED_COLOR = "#888888"
_GRID_COLOR = "#444444"
_GREEN = "#4caf50"
_RED = "#ef5350"
_ASSETS_COLOR = "#4e79a7"
_LIABILITIES_COLOR = "#e15759"
_NET_WORTH_COLOR = "#59a14f"


def _empty_figure(message: str, figsize=(6, 3)) -> Figure:
    fig = Figure(figsize=figsize, dpi=100)
    fig.patch.set_facecolor("none")
    fig.patch.set_alpha(0)
    ax = fig.add_subplot(111)
    ax.set_facecolor("none")
    ax.text(0.5, 0.5, message, ha="center", va="center",
            fontsize=13, color=_MUTED_COLOR)
    ax.set_axis_off()
    return fig


def _compact_money_tick(value, _):
    """Unsigned compact tick label, e.g. "$0", "$500", "$5K", "$1.2M", "$3.4B".

    Replaces the older `$xxxk` formatter so a $100B value renders as "$100.0B"
    rather than "$100000000k".
    """
    if value == 0:
        return "$0"
    sign = "-" if value < 0 else ""
    abs_v = abs(value)
    if abs_v < 1_000:
        return f"{sign}${abs_v:,.0f}"
    if abs_v < 1_000_000:
        return f"{sign}${abs_v / 1_000:.0f}K"
    if abs_v < 1_000_000_000:
        return f"{sign}${abs_v / 1_000_000:.1f}M"
    if abs_v < 1_000_000_000_000:
        return f"{sign}${abs_v / 1_000_000_000:.1f}B"
    return f"{sign}${abs_v / 1_000_000_000_000:.1f}T"


def _style_ax(ax, compact: bool = False):
    ax.set_facecolor("none")
    ax.tick_params(colors=_TEXT_COLOR, labelsize=7 if compact else 8)
    for spine in ax.spines.values():
        spine.set_color(_GRID_COLOR)
    ax.yaxis.set_major_formatter(_compact_money_tick)


def create_net_worth_trend_figure(
    trend_rows: list[dict], compact: bool = False,
) -> Figure:
    """Net-worth trend line chart.

    `compact=True` is intended for narrow Dashboard layouts: fewer x-axis
    ticks, slightly smaller fonts, and extra left margin so compact-money
    y-axis labels don't get clipped.
    """
    if not trend_rows:
        return _empty_figure("No snapshot history yet")

    fig = Figure(figsize=(6, 3), dpi=100)
    fig.patch.set_facecolor("none")
    fig.patch.set_alpha(0)
    ax = fig.add_subplot(111)
    _style_ax(ax, compact=compact)

    dates = [r["date"] for r in trend_rows]
    net_worth = [r["net_worth"] for r in trend_rows]
    assets = [r["total_assets"] for r in trend_rows]
    liabilities = [r["total_liabilities"] for r in trend_rows]
    has_liab = any(v > 0 for v in liabilities)

    # Smart y-range: anchoring to $0 makes a sub-percent move on a large
    # portfolio render as a flat line. If the data is bunched near the top
    # of its absolute scale (min/max > 0.5), zoom into the actual range with
    # ~15% padding above and below. Otherwise keep the floor at 0 so the
    # absolute scale stays honest.
    series = list(assets) + list(net_worth)
    if has_liab:
        series += [v for v in liabilities if v > 0]
    series_min = min(series)
    series_max = max(series)
    spread = series_max - series_min

    if spread == 0:
        anchor = abs(series_max) or 1
        pad = anchor * 0.05
        y_min = series_min - pad
        y_max = series_max + pad
    elif series_min > 0 and series_min / series_max > 0.5:
        pad = spread * 0.15
        y_min = series_min - pad
        y_max = series_max + pad
    elif series_min >= 0:
        y_min = 0
        y_max = series_max + spread * 0.1
    else:
        pad = spread * 0.15
        y_min = series_min - pad
        y_max = series_max + pad

    # Fill from the chosen floor (not 0) so the shaded band tracks the
    # zoomed view instead of dragging the axis back down.
    ax.fill_between(range(len(dates)), y_min, assets, alpha=0.08, color=_ASSETS_COLOR)
    ax.plot(range(len(dates)), assets, color=_ASSETS_COLOR,
            linewidth=1.4, alpha=0.65, label="Assets")
    if has_liab:
        ax.plot(range(len(dates)), liabilities, color=_LIABILITIES_COLOR,
                linewidth=1.4, alpha=0.65, label="Liabilities")
    ax.plot(range(len(dates)), net_worth, color=_NET_WORTH_COLOR,
            linewidth=2.2, label="Net Worth")

    ax.set_ylim(y_min, y_max)

    # Cap ticks tighter in compact mode so 45-degree dates don't pile up.
    max_ticks = 4 if compact else 6
    tick_fontsize = 6 if compact else 7
    if len(dates) <= max_ticks * 2:
        ax.set_xticks(range(len(dates)))
        ax.set_xticklabels(dates, rotation=45, ha="right",
                           fontsize=tick_fontsize, color=_TEXT_COLOR)
    else:
        step = max(1, len(dates) // max_ticks)
        ticks = list(range(0, len(dates), step))
        ax.set_xticks(ticks)
        ax.set_xticklabels([dates[i] for i in ticks],
                           rotation=45, ha="right",
                           fontsize=tick_fontsize, color=_TEXT_COLOR)

    ax.grid(axis="y", color=_GRID_COLOR, linewidth=0.5, alpha=0.5)
    # Legend gets a semi-transparent dark card. Without a frame the text
    # disappears into the plotted lines once the y-axis is zoomed in;
    # `loc="best"` lets matplotlib pick the corner with the least overlap.
    legend = ax.legend(
        fontsize=7 if compact else 8,
        loc="best",
        frameon=True,
        framealpha=0.82,
        facecolor="#1f1f1f",
        edgecolor=_GRID_COLOR,
        borderpad=0.5,
        handlelength=1.6,
    )
    legend.get_frame().set_linewidth(0.5)
    for text in legend.get_texts():
        text.set_color(_TEXT_COLOR)

    if compact:
        # Wider left margin so compact y-tick labels ("$1.2M") fit;
        # bottom kept tight so the chart isn't dominated by date labels.
        fig.subplots_adjust(left=0.16, right=0.97, top=0.92, bottom=0.24)
    else:
        fig.subplots_adjust(left=0.12, right=0.96, top=0.92, bottom=0.22)
    return fig


def create_asset_mix_figure(items: list[dict], compact: bool = False) -> Figure:
    """Asset-mix donut chart.

    `compact=True` moves the legend below the donut and shortens the asset
    name budget so a narrow Dashboard column doesn't crush the donut down to
    a few pixels. No category is dropped — top-5 + "Other" always stay.
    """
    if not items:
        return _empty_figure("No asset data")

    top = items[:5]
    rest = items[5:]
    if rest:
        other_value = sum(i["value"] for i in rest)
        total = sum(i["value"] for i in items)
        top = list(top)
        top.append({
            "name": "Other",
            "value": other_value,
            "pct": other_value / total if total else 0,
            "asset_type": "other",
        })

    name_max = 10 if compact else 18
    name_keep = 8 if compact else 16
    labels = []
    for i in top:
        name = i["name"]
        if len(name) > name_max:
            name = name[:name_keep] + ".."
        labels.append(f"{name}\n{i['pct']:.0%}")
    values = [i["value"] for i in top]
    colors = [_PALETTE[idx % len(_PALETTE)] for idx in range(len(top))]

    fig = Figure(figsize=(4, 3), dpi=100)
    fig.patch.set_facecolor("none")
    fig.patch.set_alpha(0)
    ax = fig.add_subplot(111)
    ax.set_facecolor("none")

    wedges, texts = ax.pie(
        values, colors=colors, startangle=90,
        wedgeprops=dict(width=0.45, edgecolor="white", linewidth=0.5),
    )

    if compact:
        # Legend below the donut, in 2-3 columns so it doesn't run tall.
        ncol = max(1, min(3, len(top)))
        legend = ax.legend(
            wedges, labels, loc="upper center",
            bbox_to_anchor=(0.5, -0.02), fontsize=7, frameon=False,
            ncol=ncol, handletextpad=0.4, columnspacing=0.8,
        )
        # Donut keeps the upper area; legend gets the bottom strip.
        fig.subplots_adjust(left=0.05, right=0.95, top=0.95, bottom=0.25)
    else:
        legend = ax.legend(
            wedges, labels, loc="center left", bbox_to_anchor=(1.0, 0.5),
            fontsize=8, frameon=False,
        )
        fig.subplots_adjust(left=0.02, right=0.55, top=0.95, bottom=0.05)
    for text in legend.get_texts():
        text.set_color(_TEXT_COLOR)

    ax.set_aspect("equal")
    return fig


def _fmt_pnl_tick(x, _):
    """Signed compact tick label for unrealized-PnL bars: "$0", "$+500",
    "$-1,200", "$+5K", "$-3.4M", etc."""
    if x == 0:
        return "$0"
    sign = "+" if x > 0 else "-"
    abs_x = abs(x)
    if abs_x < 1_000:
        return f"${sign}{abs_x:,.0f}"
    if abs_x < 1_000_000:
        return f"${sign}{abs_x / 1_000:.0f}K"
    if abs_x < 1_000_000_000:
        return f"${sign}{abs_x / 1_000_000:.1f}M"
    if abs_x < 1_000_000_000_000:
        return f"${sign}{abs_x / 1_000_000_000:.1f}B"
    return f"${sign}{abs_x / 1_000_000_000_000:.1f}T"


def create_return_drivers_figure(
    gainers: list[dict], losers: list[dict], compact: bool = False,
) -> Figure:
    """Horizontal bar chart of unrealized PnL per position.

    `compact=True` shrinks fonts and widens the x-axis padding so bar
    annotations don't overflow the plot in a narrow column. Bar colors
    (green / red) and the zero line are kept so positive vs negative
    semantics are obvious even when labels get tight.
    """
    items = list(gainers) + list(losers)
    if not items:
        return _empty_figure("No priced positions yet")

    items.sort(key=lambda x: x["unrealized_pnl"])
    symbols = [i["symbol"] for i in items]
    pnl_values = [i["unrealized_pnl"] for i in items]
    bar_colors = [_GREEN if v >= 0 else _RED for v in pnl_values]

    fig_height = max(2, len(items) * 0.4 + 0.8)
    fig = Figure(figsize=(6, fig_height), dpi=100)
    fig.patch.set_facecolor("none")
    fig.patch.set_alpha(0)
    ax = fig.add_subplot(111)
    ax.set_facecolor("none")
    tick_size = 7 if compact else 8
    ax.tick_params(colors=_TEXT_COLOR, labelsize=tick_size)
    for spine in ax.spines.values():
        spine.set_color(_GRID_COLOR)

    bars = ax.barh(symbols, pnl_values, color=bar_colors, height=0.6, edgecolor="none")

    annotation_size = 7 if compact else 8
    annotation_offset = 4 if compact else 6
    for bar, val in zip(bars, pnl_values):
        offset_pts = (annotation_offset, 0) if val >= 0 else (-annotation_offset, 0)
        ha = "left" if val >= 0 else "right"
        label = _fmt_pnl_tick(val, None)
        ax.annotate(
            label,
            xy=(bar.get_width(), bar.get_y() + bar.get_height() / 2),
            xytext=offset_pts,
            textcoords="offset points",
            va="center", ha=ha, fontsize=annotation_size, color=_TEXT_COLOR,
        )

    # In compact mode, leave more horizontal pad so the bar annotations
    # don't get clipped by a narrow plot area.
    max_abs = max(abs(v) for v in pnl_values)
    pad_factor = 0.35 if compact else 0.25
    pad = max(max_abs * pad_factor, 1.0)
    ax.set_xlim(-max_abs - pad if any(v < 0 for v in pnl_values) else -pad,
                max_abs + pad if any(v >= 0 for v in pnl_values) else pad)

    ax.axvline(0, color=_GRID_COLOR, linewidth=0.8)
    ax.grid(axis="x", color=_GRID_COLOR, linewidth=0.5, alpha=0.3)
    ax.xaxis.set_major_formatter(_fmt_pnl_tick)

    fig.tight_layout(pad=0.8 if compact else 1.0)
    return fig
