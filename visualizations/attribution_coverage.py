import matplotlib.pyplot as plt
import numpy as np

apps = ['OTel Demo', 'Train Ticket', 'Social Network', 'Online Boutique']
workstation = [68.79, 60.58, 8.60, 0.00]
cloud = [51.49, 37.88, 0.87, 0.00]

y = np.arange(len(apps))
height = 0.35

fig, ax = plt.subplots(figsize=(6.5, 3.2))

bars_ws = ax.barh(y + height/2, workstation, height, label='Workstation (RAPL)', color='#1f5fa8')
bars_cl = ax.barh(y - height/2, cloud, height, label='EC2 (eBPF)', color='#a8c6e8')

ax.set_yticks(y)
ax.set_yticklabels(apps)
ax.invert_yaxis()
ax.set_xlabel('M1 attributed energy as % of total measured system energy (M2 $E_{total}$)')
ax.set_xlim(0, 80)
ax.legend(loc='lower right', frameon=False)

for bars in [bars_ws, bars_cl]:
    for bar in bars:
        width = bar.get_width()
        label = f'{width:.1f}%' if width > 0 else '0%'
        ax.text(width + 1.2, bar.get_y() + bar.get_height()/2, label,
                va='center', ha='left', fontsize=9)

ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
plt.tight_layout()
plt.savefig('m1_coverage_chart.pdf', dpi=300, bbox_inches='tight')
plt.savefig('m1_coverage_chart.png', dpi=200, bbox_inches='tight')
print("saved")