import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl

# 设置是否显示中位数
SHOW_MEDIAN = True  # 设置为True显示中位数，False则不显示

# 设置科研风格
mpl.rcParams['font.family'] = 'Times New Roman'
mpl.rcParams['font.size'] = 12
mpl.rcParams['axes.linewidth'] = 1.5
mpl.rcParams['axes.grid'] = True
mpl.rcParams['grid.linestyle'] = '--'
mpl.rcParams['grid.alpha'] = 0.7
mpl.rcParams['xtick.major.width'] = 1.5
mpl.rcParams['ytick.major.width'] = 1.5
mpl.rcParams['xtick.minor.width'] = 1.0
mpl.rcParams['ytick.minor.width'] = 1.0
mpl.rcParams['xtick.direction'] = 'in'
mpl.rcParams['ytick.direction'] = 'in'

# 读取数据
df = pd.read_csv('Event study_results/异常收益率（AR）&累积异常收益率(CAR).csv')

# 按relative_day分组并计算均值和中位数
ar_means = df.groupby('relative_day')['abnormal_return'].mean()
ar_medians = df.groupby('relative_day')['abnormal_return'].median()
car_means = df.groupby('relative_day')['cumulative_abnormal_return'].mean()
car_medians = df.groupby('relative_day')['cumulative_abnormal_return'].median()

# 创建图形
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 12))

# 绘制AR均值点线图
ax1.plot(ar_means.index, ar_means.values, 'o-', color='#1f77b4', 
        linewidth=2, markersize=8, markerfacecolor='white', 
        markeredgewidth=2, markeredgecolor='#1f77b4', label='Mean')

# 如果需要显示AR中位数
if SHOW_MEDIAN:
    ax1.plot(ar_medians.index, ar_medians.values, 's--', color='#ff7f0e',
            linewidth=2, markersize=8, markerfacecolor='white',
            markeredgewidth=2, markeredgecolor='#ff7f0e', label='Median')

# 添加零线
ax1.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.7)

# 设置坐标轴标签
ax1.set_xlabel('Relative Day', fontsize=14)
ax1.set_ylabel('Abnormal Return', fontsize=14)

# 设置标题
ax1.set_title('Average Abnormal Return Trend Around Event Day', fontsize=16, pad=20)

# 设置网格
ax1.grid(True, linestyle='--', alpha=0.7)

# 设置x轴刻度和标签
xticks = ar_means.index
xticklabels = [str(x) if x != 0 else 'Event day' for x in xticks]
ax1.set_xticks(xticks)
ax1.set_xticklabels(xticklabels)

# 添加事件日标记
ax1.axvline(x=0, color='red', linestyle='--', linewidth=1.5, alpha=0.7)

# 添加图例
ax1.legend(fontsize=12)

# 绘制CAR均值点线图
ax2.plot(car_means.index, car_means.values, 'o-', color='#1f77b4', 
        linewidth=2, markersize=8, markerfacecolor='white', 
        markeredgewidth=2, markeredgecolor='#1f77b4', label='Mean')

# 如果需要显示CAR中位数
if SHOW_MEDIAN:
    ax2.plot(car_medians.index, car_medians.values, 's--', color='#ff7f0e',
            linewidth=2, markersize=8, markerfacecolor='white',
            markeredgewidth=2, markeredgecolor='#ff7f0e', label='Median')

# 添加零线
ax2.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.7)

# 设置坐标轴标签
ax2.set_xlabel('Relative Day', fontsize=14)
ax2.set_ylabel('Cumulative Abnormal Return', fontsize=14)

# 设置标题
ax2.set_title('Average Cumulative Abnormal Return Trend Around Event Day', fontsize=16, pad=20)

# 设置网格
ax2.grid(True, linestyle='--', alpha=0.7)

# 设置x轴刻度和标签
ax2.set_xticks(xticks)
ax2.set_xticklabels(xticklabels)

# 添加事件日标记
ax2.axvline(x=0, color='red', linestyle='--', linewidth=1.5, alpha=0.7)

# 添加图例
ax2.legend(fontsize=12)

# 调整布局
plt.tight_layout()

# 保存图形
plt.savefig('Event study_results/异常收益率（AR）&累积异常收益率(CAR)_graph.png', dpi=300, bbox_inches='tight')
# plt.savefig('事件研究法/Event Study Results/ar_trend.pdf', bbox_inches='tight')

# 显示图形
plt.show() 