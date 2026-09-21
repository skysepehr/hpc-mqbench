#!/usr/bin/env python3
"""Plot Kafka screening evidence from the copied, unchanged result table.

Run with a Python environment containing matplotlib. Writes one vector PDF.
All 120 observations appear in the throughput panel; the latency panel omits
seven observations whose latency measurements failed the eligibility gate.
"""
from pathlib import Path
import csv

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import FixedLocator, FuncFormatter, NullLocator

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'data/kafka/derived/phase1_configuration_outcomes.csv'
DEST = ROOT / 'figures/kafka_operating_limits.pdf'
rows = list(csv.DictReader(SOURCE.open()))
assert len(rows) == 120
eligible = [r for r in rows if r['eligible'] == 'True']
qualified = [r for r in eligible if r['qualified'] == 'True']
overdriven = [r for r in eligible if r['qualified'] != 'True']
ineligible = [r for r in rows if r['eligible'] != 'True']
assert (len(eligible), len(qualified), len(overdriven), len(ineligible)) == (113, 99, 14, 7)
assert all(r['latency_valid'] == 'False' for r in ineligible)

plt.rcParams.update({
    'font.family': 'DejaVu Sans', 'font.size': 8.5,
    'axes.titlesize': 9.5, 'axes.labelsize': 8.5,
    'xtick.labelsize': 8, 'ytick.labelsize': 8,
    'pdf.fonttype': 42, 'ps.fonttype': 42,
    'axes.spines.top': False, 'axes.spines.right': False,
    'axes.linewidth': 0.7, 'grid.linewidth': 0.45,
})
fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.8))
colors = {'Qualified': '#007c83', 'Overdriven': '#ba531c', 'Ineligible': '#73777c'}
styles = [(qualified, 'Qualified', 'o'), (overdriven, 'Overdriven', '^'),
          (ineligible, 'Ineligible', 'x')]
for i, (ax, field, ylabel) in enumerate(zip(
        axes, ['balanced_mib_per_sec', 'latency_p99_ms'],
        ['Balanced endpoint rate (MiB/s)', 'End-to-end p99 (ms)'])):
    for group, label, marker in styles:
        if i == 1 and label == 'Ineligible':
            continue
        ax.scatter([float(r['backlog_percent']) for r in group],
                   [float(r[field]) for r in group],
                   c=colors[label], marker=marker, s=20 if marker == 'o' else 26,
                   alpha=0.74 if marker == 'o' else 0.95,
                   linewidths=0.7 if marker == 'x' else 0.35,
                   edgecolors=None if marker == 'x' else 'white', zorder=3)
    ax.set_xscale('log')
    ax.set_xlim(0.01, 100)
    ax.xaxis.set_major_locator(FixedLocator([0.01, 0.1, 1, 5, 100]))
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{x:g}'))
    ax.xaxis.set_minor_locator(NullLocator())
    ax.axvline(5, color='#3e4246', linestyle=(0, (4, 3)), linewidth=0.85, zorder=2)
    ax.grid(True, which='major', color='#e0e2e4', zorder=0)
    ax.set_xlabel('Producer pending deliveries (%)')
    ax.set_ylabel(ylabel)
    ax.set_title('(a) Endpoint rate: all 120 cases' if i == 0 else '(b) Latency: 113 eligible cases', loc='left', pad=7)
axes[0].set_ylim(0, 3575)
axes[0].yaxis.set_major_locator(FixedLocator([0, 1000, 2000, 3000]))
axes[1].set_yscale('log')
axes[1].set_ylim(10, 70000)
axes[1].yaxis.set_major_locator(FixedLocator([10, 100, 1000, 10000]))
axes[1].yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{int(x):,}'))
axes[1].yaxis.set_minor_locator(NullLocator())
by_id = {r['config_id']: r for r in rows}
def note(panel, case, text, xytext, ha='left'):
    r = by_id[case]
    yfield = 'balanced_mib_per_sec' if panel == 0 else 'latency_p99_ms'
    axes[panel].annotate(text, (float(r['backlog_percent']), float(r[yfield])),
        xytext=xytext, textcoords='offset points', fontsize=7,
        ha=ha, va='center', color='#25272a',
        arrowprops=dict(arrowstyle='-', color='#73777c', lw=0.6),
        bbox=dict(facecolor='white', edgecolor='none', alpha=0.92, pad=1), zorder=5)
note(0, 'cfg_001', '001 (4 KiB)', (-38, 18))
note(0, 'cfg_096', '096 (16 KiB)', (-44, 11))
note(0, 'cfg_107', '107 (8 KiB)', (-50, 21))
note(0, 'cfg_101', '101 (16 KiB)', (12, 18))
note(1, 'cfg_090', '090 (8 KiB)', (14, 0))
note(1, 'cfg_007', '007 (4 KiB)', (-50, 24))
handles = [Line2D([0], [0], marker=marker, color='none', markeredgecolor=colors[label],
           markerfacecolor=colors[label], markersize=5, label=f'{label} ({len(group)})')
           for group, label, marker in styles]
fig.legend(handles=handles, loc='upper center', bbox_to_anchor=(0.51, 1.00),
           ncol=3, frameon=False, handletextpad=0.3, columnspacing=1.8, fontsize=8)
fig.subplots_adjust(left=0.084, right=0.972, bottom=0.19, top=0.79, wspace=0.30)
DEST.parent.mkdir(parents=True, exist_ok=True)
fig.savefig(DEST, metadata={'Title': 'Kafka screening: throughput, pending deliveries, and latency',
    'Author': 'HPC-MQBench', 'Subject': '120-case screening with eligibility and qualification status',
    'Keywords': 'Kafka, benchmark, throughput, delivery backlog, latency'})
plt.close(fig)
print(f'Wrote {DEST}')
