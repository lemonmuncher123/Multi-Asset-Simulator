import weakref
from matplotlib.figure import Figure

_PALETTE = [
    "#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f",
    "#edc948", "#b07aa1", "#ff9da7", "#9c755f", "#bab0ac",
    "#86bcb6", "#8cd17d", "#b6992d", "#499894", "#d37295",
    "#a0cbe8", "#ffbe7d", "#d4a6c8", "#fabfd2", "#d7b5a6",
]

_TEXT_COLOR = "#cccccc"
_TOOLTIP_BG = "#2b2b2b"
_TOOLTIP_BORDER = "#555555"
_TOOLTIP_TEXT = "#e0e0e0"
_EDGE_NORMAL = ("white", 0.5)
_EDGE_HIGHLIGHT = ("#ffffff", 2.5)

_TYPE_LABELS = {
    "stock": "Stock", "etf": "ETF", "crypto": "Crypto",
    "real_estate": "Real Estate", "cash": "Cash", "custom": "Custom",
}


def create_asset_pie_figure(items: list[dict], compact: bool = False) -> Figure:
    """Asset-allocation pie chart.

    `compact=True` is intended for narrow containers (e.g. the Asset Analysis
    page where the pie shares its row with the cashflow chart). In compact
    mode the legend moves below the pie so the pie can use the full width
    of its column instead of shrinking to fit alongside a side-legend.
    """
    fig = Figure(figsize=(6, 4), dpi=100)
    fig.patch.set_facecolor("none")
    fig.patch.set_alpha(0)

    if not items:
        ax = fig.add_subplot(111)
        ax.set_facecolor("none")
        ax.text(0.5, 0.5, "No asset data", ha="center", va="center",
                fontsize=14, color="#888888")
        ax.set_axis_off()
        return fig

    labels = [i["name"] for i in items]
    values = [i["value"] for i in items]
    colors = [_PALETTE[idx % len(_PALETTE)] for idx in range(len(items))]

    ax = fig.add_subplot(111)
    ax.set_facecolor("none")
    # Inline label threshold: only slices ≥7% get a percentage drawn on the
    # wedge. Below that, two adjacent thin slices place their label texts
    # close enough to overlap. Smaller slices are still visible in the
    # legend (which now includes the percentage) and on hover.
    wedges, texts, autotexts = ax.pie(
        values,
        colors=colors,
        autopct=lambda pct: f"{pct:.1f}%" if pct >= 7 else "",
        startangle=90,
        pctdistance=0.75,
    )
    for t in autotexts:
        t.set_color("white")
        t.set_fontweight("bold")
        t.set_fontsize(9)
    for w in wedges:
        w.set_edgecolor(_EDGE_NORMAL[0])
        w.set_linewidth(_EDGE_NORMAL[1])
    ax.set_aspect("equal")
    # Anchor "N": when the subplot bbox is taller than the equal-aspect
    # pie's natural square, the pie hugs the top of its bbox instead of
    # floating in the middle with empty space above it.
    ax.set_anchor("N")

    # Legend includes the percentage so slices below the inline-label
    # threshold are still readable.
    total = sum(values) or 1.0
    legend_labels = [
        f"{name}  {value / total:.1%}"
        for name, value in zip(labels, values)
    ]

    if compact:
        # Legend below the pie, in 2-3 columns. Frees horizontal space so
        # the pie can fill the full column width instead of being squeezed
        # to leave room for a side legend.
        ncol = max(1, min(3, len(legend_labels)))
        legend = ax.legend(
            wedges, legend_labels, loc="upper center",
            bbox_to_anchor=(0.5, -0.02),
            fontsize=8, frameon=False, ncol=ncol,
            handletextpad=0.4, columnspacing=0.8,
        )
        fig.subplots_adjust(left=0.02, right=0.98, top=0.96, bottom=0.22)
    else:
        legend = ax.legend(
            wedges, legend_labels,
            loc="center left",
            bbox_to_anchor=(1.05, 0.5),
            fontsize=9,
            frameon=False,
        )
        fig.subplots_adjust(left=0.02, right=0.62, top=0.95, bottom=0.05)
    for text in legend.get_texts():
        text.set_color(_TEXT_COLOR)

    fig._pie_hover_data = (wedges, items)

    return fig


def connect_pie_hover(canvas):
    fig = canvas.figure
    if not hasattr(fig, "_pie_hover_data"):
        return None

    wedges, items = fig._pie_hover_data
    if not wedges:
        return None

    ax = wedges[0].axes
    annotation = ax.annotate(
        "", xy=(0, 0), xytext=(20, 20), textcoords="offset points",
        bbox=dict(
            boxstyle="round,pad=0.6", fc=_TOOLTIP_BG,
            ec=_TOOLTIP_BORDER, alpha=0.95,
        ),
        color=_TOOLTIP_TEXT, fontsize=9, zorder=10,
    )
    annotation.set_visible(False)

    state = {"idx": None}
    canvas_ref = weakref.ref(canvas)

    def _aim_tooltip(event):
        """Flip the tooltip's vertical direction based on cursor position.

        The pie is anchored to the top of its canvas (`ax.set_anchor("N")`),
        so a fixed up-right offset clips the tooltip's top line for slices
        near the pie's top edge. Flipping to down-right when the cursor is
        in the upper half keeps the tooltip on-canvas.
        """
        ax_bbox = ax.get_window_extent()
        if ax_bbox.height <= 0:
            return
        y_frac = (event.y - ax_bbox.y0) / ax_bbox.height
        if y_frac > 0.5:
            annotation.set_position((20, -20))
            annotation.set_va("top")
        else:
            annotation.set_position((20, 20))
            annotation.set_va("bottom")

    def _on_move(event):
        c = canvas_ref()
        if c is None:
            return
        if event.inaxes != ax or event.xdata is None:
            if state["idx"] is not None:
                _reset(state["idx"])
                state["idx"] = None
                annotation.set_visible(False)
                c.draw_idle()
            return

        for i, wedge in enumerate(wedges):
            hit, _ = wedge.contains(event)
            if hit:
                if state["idx"] == i:
                    annotation.xy = (event.xdata, event.ydata)
                    _aim_tooltip(event)
                    c.draw_idle()
                    return
                if state["idx"] is not None:
                    _reset(state["idx"])
                wedges[i].set_edgecolor(_EDGE_HIGHLIGHT[0])
                wedges[i].set_linewidth(_EDGE_HIGHLIGHT[1])
                state["idx"] = i

                item = items[i]
                type_label = _TYPE_LABELS.get(
                    item["asset_type"],
                    item["asset_type"].replace("_", " ").title(),
                )
                text = (
                    f"{item['name']}\n"
                    f"Type: {type_label}\n"
                    f"Value: ${item['value']:,.2f}\n"
                    f"Allocation: {item['pct']:.1%}"
                )
                annotation.set_text(text)
                annotation.xy = (event.xdata, event.ydata)
                _aim_tooltip(event)
                annotation.set_visible(True)
                c.draw_idle()
                return

        if state["idx"] is not None:
            _reset(state["idx"])
            state["idx"] = None
            annotation.set_visible(False)
            c.draw_idle()

    def _reset(idx):
        wedges[idx].set_edgecolor(_EDGE_NORMAL[0])
        wedges[idx].set_linewidth(_EDGE_NORMAL[1])

    return canvas.mpl_connect("motion_notify_event", _on_move)
