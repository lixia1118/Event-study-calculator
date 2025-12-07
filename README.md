# Event Study Analysis System

一个基于 Python 的事件研究法（Event Study）分析项目，用于计算上市公司在特定事件发生前后的异常收益率（AR）和累积异常收益率（CAR），并进行统计检验。本项目支持**多进程并行计算**，能够高效处理大规模的事件数据。

# 事件研究法（Event Study）

## 什么是事件研究法？

事件研究法是经济学、金融学、会计学中常用的一种分析方法，被用于研究某一特定事件对上市公司股票收益率的影响。
特定事件可以是公司控制范围内的事件，例如宣布股票分割、定增计划、并购重组等，也可以是公司不可控的事件，例如某个法案的通过、监管处罚、或者国际事件(俄乌冲突)等。这些事件都可能直接或间接的影响公司经营。

## 短期事件研究法依赖的三个基本假设：
+ 根据有效市场假说 (Efficient Markets Hypothesis, EMH) , 金融市场是有效的, 即股票价格反映所有已知的公共信息
+ 所研究的事件是市场未预期到的，因此这种异常收益可以度量股价对事件发生或信息披露异常反应的程度
+ 在事件发生的窗口期间无其他事件的混合效应

## 事件研究法的基本步骤：
+ 整理事件列表，事件列表中包含了公司名称和事件发生日期。
+ 整理事件列表中的公司在事件发生日前后的收益率数据和同一时期的市场收益率数据。
+ 根据模型估计事件窗口期的预期收益率并比较公司在事件窗口期的实际收益率与预期收益率的差异。
+ 通过回归来检验特定事件对公司股票收益率是否有显著的影响（$t$检验）。

# 本代码相关

## ✨ 功能特点

### 核心功能
- **异常收益率计算**：基于管理学中最常见的资本资产定价模型（CAPM）计算个股的异常收益率（AR）和累积异常收益率（CAR）
- **多进程并行计算**：利用 Python `multiprocessing` 模块实现多核心并行处理，显著提升计算速度
- **断点续传**：自动跳过已处理的事件，支持中断后继续运行，避免重复计算
- **实时进度显示**：使用 `tqdm` 显示实时进度条，包括处理速度、成功/失败统计等信息

### 数据处理
- **窗口配置**：支持自定义事件窗口（Event Window）和估计窗口（Estimation Window）的长度及间隔. 如果将事件日在时间轴上视为0点，那么**默认事件窗口为[-3, 5]（即事件日前3与后5个交易日），默认估计窗口为[-128, -9]（即120个交易日）**. 

  ```markdown
  > [!NOTE]
  > 当事件日不是交易日时（事件日在节假日），自动使用事件日后的第一个交易日作为0点
  ```

  

- **多种指数支持**：**默认是沪深300**，可以自动根据股票代码匹配对应的市场指数（上证综指、深证综指、沪深300），也支持自定义指数

- **数据持久化**：使用 SQLite 数据库存储股票日行情数据，提高查询效率

### 统计分析
- **统计检验**：提供详细的统计检验脚本，包括：
  - $t$ 检验（检验均值是否为0）
  - Wilcoxon 符号秩检验（检验中位数是否为0）
  - 二项式符号检验（检验正负比例是否为0.5）
- **结果可视化**：生成 AR 和 CAR 的趋势图，支持显示均值和中位数

## 🏗️ 技术框架

### 核心算法
- **市场模型（Market Model）**：使用 OLS 回归估计正常收益率
  
  $$
  R_{it}=\alpha_i+\beta_i\times R_{mt}+\epsilon_{it}
  $$
  
  其中：
  - $R_{it}$：股票 $i$ 在 $t$ 日的收益率
  - $R_{mt}$：市场指数在 $t$ 日的收益率
  - $α_i, β_i$：通过估计窗口的 OLS 回归得到
  - $\epsilon_{it}$：残差项
  
- **异常收益率计算**：
  
  $$
  AR_{it}=R_{i,t}-(\alpha_i+\beta_i\times R_{m,t})
  $$
  
  $$
  CAR(t_1,t_2)=\sum _{t=t _1}^{t=t _2}AR _{i,t}
  $$

### 技术栈
- **数据处理**：pandas, numpy
- **统计分析**：statsmodels, scipy
- **数据库**：SQLite3
- **并行计算**：multiprocessing
- **可视化**：matplotlib
- **进度显示**：tqdm

## 📁 项目结构

```
Event study/
├── Event study_code/                    # 代码目录
│   ├── Event study program/
│   │   └── Event study program.py      # 主程序：计算 AR 和 CAR
│   ├── import to sqlite/
│   │   └── import_to_sqlite.py         # 数据导入脚本：将 CSV 数据导入 SQLite
│   ├── Statistic test/
│   │   └── AAR_ACAR_Statistic Test.py  # 统计检验脚本：计算 AAR、CAAR 并进行检验
│   └── plot/
│       └── plot_AAR & ACAR.py          # 可视化脚本：绘制 AR 和 CAR 趋势图
│
├── Event study_data/                    # 输入数据目录
│   ├── event info.csv                  # 事件列表（必须包含"company_code"（公司证券代码）和"event_date"（事件时间）列）
│   └── stock_daily_data.db             # 股票日行情数据库 (SQLite) **[需要生成]**
│
├── 公司数据集/                           # 原始数据目录
│   ├── 日个股回报率/
│   │   └── TRD_Daily_20150101-20250101.csv  # 股票日交易数据（需要自行从CSMAR下载）
│   └── 指数文件1990-12-19 - 2025-04-24/
│       └── TRD_Index.xlsx              # 市场指数数据（需要自行从CSMAR下载）
│
├── Event study_results/                 # 输出结果目录
│   ├── 异常收益率（AR）&累积异常收益率(CAR).csv
│   ├── 异常收益率（AR）&累积异常收益率(CAR)_统计检验结果_按相对日期分类.csv
│   ├── 异常收益率（AR）&累积异常收益率(CAR)_graph.png
│   └── 平均累计异常收益率（Mean CAR）_统计检验结果_按公司与事件日分类.csv
│
├── README.md                            # 项目说明文档
└── .gitignore                           # Git 忽略文件配置
```

## 🔧 环境依赖

### Python 版本
- Python 3.9.13

### 必需的 Python 库
```bash
pandas>=2.2.3
numpy>=1.24.4
statsmodels>=0.14.4
scipy>=1.9.1
matplotlib>=3.9.4
tqdm>=4.64.1
```

### 内置模块
- `sqlite3`（Python 标准库）
- `multiprocessing`（Python 标准库）
- `logging`（Python 标准库）

## 📦 安装指南

### 1. 克隆或下载项目
```bash
git clone <repository-url>
cd "Event study - 副本"
```

### 2. 安装依赖
```bash
pip install pandas numpy statsmodels scipy matplotlib tqdm openpyxl
```

### 3. 准备数据文件

确保以下数据文件存在于正确的位置：

- **事件数据**：`Event study_data/event info.csv`
  - 必须包含列：`company_code`（公司代码）、`event_date`（事件时间）
  - 可选列：`company_year`（公司年份）、`industry_code`（公司行业代码）、`company_province`（公司省份）、`company_city`（公司城市）（会被添加到结果中）

- **股票数据**：`公司数据集/日个股回报率/TRD_Daily_20150101-20250101.csv`**（需要自己从CSMAR下载）**
  - 从 CSMAR 数据库下载的日个股回报率数据（数据量太大，故使用csv格式加快读取速度）

- **指数数据**：`公司数据集/指数文件1990-12-19 - 2025-04-24/TRD_Index.xlsx`**（需要自己从CSMAR下载）**
  - 从 CSMAR 数据库下载的市场指数数据

## 🚀 使用说明

### 步骤 1：导入数据到 SQLite 数据库

首先，将 CSV 格式的股票数据导入到 SQLite 数据库中，以提高后续查询效率：

```bash
python "Event study_code/import to sqlite/import_to_sqlite.py"
```

**功能说明**：
- 读取 `公司数据集/日个股回报率/TRD_Daily_20150101-20250101.csv`
- 创建 SQLite 数据库：`Event study_data/stock_daily_data.db`
- 建立索引以优化查询性能
- 显示导入进度

**注意事项**：
- 如果数据库已存在，脚本会先删除旧数据库再创建新的
- 导入过程可能需要较长时间，取决于数据量大小

### 步骤 2：运行主程序计算 AR 和 CAR

运行主程序开始计算异常收益率和累积异常收益率：

```bash
python "Event study_code/Event study program/Event study program.py"
```

**功能说明**：
- 读取事件列表：`Event study_data/event info.csv`
- 自动检测已处理的事件，跳过重复计算（支持断点续传）
- 使用多进程并行计算（默认使用 CPU 核心数 - 6）
- 实时显示进度条，包括处理速度、成功/失败统计
- 结果实时保存到 CSV 文件

**输出文件**：
- `Event study_results/异常收益率（AR）&累积异常收益率(CAR).csv`
- `Event study_results/平均累计异常收益率（Mean CAR）_统计检验结果_按公司与事件日分类.csv`

### 步骤 3：运行统计检验

计算完成后，运行统计检验脚本对结果进行汇总和统计检验：

```bash
python "Event study_code/Statistic test/AAR_ACAR_Statistic Test.py"
```

**功能说明**：
- 读取 AR 和 CAR 数据
- 按相对日期（`relative_day`）分组计算统计量
- 进行三种统计检验：$t$ 检验、Wilcoxon 符号秩检验、二项式符号检验
- 计算均值、中位数、正负比例等统计量

**输出文件**：
- `Event study_results/异常收益率（AR）&累积异常收益率(CAR)_统计检验结果_按相对日期分类.csv`

### 步骤 4：生成可视化图表（可选）

生成 AR 和 CAR 的趋势图：

```bash
python "Event study_code/plot/plot_AAR & ACAR.py"
```

**功能说明**：
- 绘制平均异常收益率（AAR）和累积平均异常收益率（CAAR）的趋势图
- 支持显示均值和中位数（可在脚本中配置）
- 使用科研风格的图表样式

**输出文件**：
- `Event study_results/异常收益率（AR）&累积异常收益率(CAR)_graph.png`

## ⚙️ 参数配置

### EventStudy 类参数

在 `Event study_code/Event study program/Event study program.py` 的 `process_event_wrapper` 函数中，可以修改 `EventStudy` 类的初始化参数：

```python
event_study = EventStudy(
    event_window_before=3,        # 事件日前 3 个交易日
    event_window_after=5,         # 事件日后 5 个交易日
    estimation_window_length=120, # 估计窗口长度 120 个交易日
    estimation_window_gap=5       # 估计窗口与事件窗口间隔 5 个交易日
)
```

**参数说明**：
- `event_window_before`：事件窗口开始日相对于事件日的天数（负数，如 -3 表示事件日前 3 天）
- `event_window_after`：事件窗口结束日相对于事件日的天数（正数，如 5 表示事件日后 5 天）
- `estimation_window_length`：估计窗口的长度（交易日数）
- `estimation_window_gap`：估计窗口结束日与事件窗口开始日之间的间隔天数

**窗口示意图**：

![Window Description](https://github.com/lixia1118/Event-study-calculator/blob/main/Github%20graphs/Event%20window.png)

### 多进程配置

在 `main()` 函数中，可以调整并行计算的进程数：

```python
num_processes = max(1, multiprocessing.cpu_count() - 6)  # 保留 6 个核心给系统
```

**建议**：
- 根据 CPU 核心数调整保留的核心数
- 如果系统资源充足，可以减少保留的核心数
- 如果系统资源紧张，可以增加保留的核心数

### 市场指数配置

在 `process_event_wrapper` 函数中，可以指定使用的市场指数：

```python
merged_data = event_study.get_data(company_code, event_date, custom_index='000300')
```

**指数代码**：
- `'000300'`：沪深300指数（默认）
- `'000001'`：上证综合指数
- `'399106'`：深证综合指数
- `None`：自动根据股票代码匹配对应的市场指数

## 📊 输出结果

### 1. AR 和 CAR 数据文件

**文件**：`异常收益率（AR）&累积异常收益率(CAR).csv`

**列说明**：
- `company_code`：公司代码
- `event_date`：事件日期
- `relative_day`：相对日期（事件日为 0，事件日前为负数，事件日后为正数）
- `abnormal_return`：异常收益率（AR）
- `cumulative_abnormal_return`：累积异常收益率（CAR）
- `公司年份`、`公司行业代码`、`公司省份`、`公司城市`：从事件数据中继承的额外信息

### 2. 统计检验结果（按相对日期分类）

**文件**：`异常收益率（AR）&累积异常收益率(CAR)_统计检验结果_按相对日期分类.csv`

**统计量说明**：
- `AR_Mean` / `CAR_Mean`：均值
- `AR_Median` / `CAR_Median`：中位数
- `AR_Min` / `CAR_Min`：最小值
- `AR_Max` / `CAR_Max`：最大值
- `AR_t_stat` / `CAR_t_stat`：t 检验统计量
- `AR_p_value` / `CAR_p_value`：t 检验 p 值
- `AR_w_stat` / `CAR_w_stat`：Wilcoxon 检验统计量
- `AR_w_p_value` / `CAR_w_p_value`：Wilcoxon 检验 p 值
- `AR_Positive_Ratio` / `CAR_Positive_Ratio`：正收益率比例
- `AR_Negative_Ratio` / `CAR_Negative_Ratio`：负收益率比例
- `AR_Binom_p_value_Positive` / `CAR_Binom_p_value_Positive`：二项式检验 p 值（正收益率）
- `AR_Binom_p_value_Negative` / `CAR_Binom_p_value_Negative`：二项式检验 p 值（负收益率）

### 3. 统计检验结果（按公司与事件日分类）

**文件**：`平均累计异常收益率（Mean CAR）_统计检验结果_按公司与事件日分类.csv`

**列说明**：
- `company_code`：公司代码
- `event_date`：事件日期
- `mean_CAR`：平均累积异常收益率
- `t_stat`：t 检验统计量
- `p_value`：t 检验 p 值
- `significant`：是否显著（基于 $p$ 值 < 0.05）

### 4. 可视化图表

**文件**：`异常收益率（AR）&累积异常收益率(CAR)_graph.png`

包含两个子图：
- 上图：平均异常收益率（AAR）趋势
- 下图：累积平均异常收益率（CAAR）趋势

![AR&CAR](https://github.com/lixia1118/Event-study-calculator/blob/main/Github%20graphs/AR%20%26%20CAR_graph.png)

## ⚠️ 注意事项

### 数据要求
1. **事件数据格式**：
   - CSV 文件必须包含 `公司代码` 和 `事件时间` 列
   - `事件时间` 格式应为 `YYYY-MM-DD` 或 `YYYY/MM/DD`的`datetime`格式
2. **股票数据要求**：
   - 数据应包含股票代码、交易日期、收益率等必要字段
   - 确保数据时间范围覆盖所有事件日期
3. **指数数据要求**：
   - 确保指数数据的时间范围覆盖所有事件日期
   - 指数代码应与股票代码匹配（沪市股票对应上证指数，深市股票对应深证指数）

### 运行环境
1. **Windows 系统**：
   - 使用 `multiprocessing.freeze_support()` 确保多进程正常工作
   - 确保在 `if __name__ == '__main__':` 块中运行主程序

2. **内存管理**：
   - 大规模数据处理时，注意内存使用情况
   - 程序采用实时写入 CSV 的方式，避免内存溢出

3. **日志文件**：
   - 日志文件保存在 `Event study_code/Event study program/event_study.log`
   - 如果程序异常，可以查看日志文件获取详细信息

### 性能优化
1. **多进程设置**：
   - 根据 CPU 核心数合理设置并行进程数
   - 过多进程可能导致系统资源竞争，反而降低效率

2. **数据库索引**：
   - SQLite 数据库已建立索引，查询速度较快
   - 如果数据量特别大，可以考虑使用更专业的数据库（如 PostgreSQL）

3. **断点续传**：
   - 程序支持断点续传，可以安全地中断和重启
   - 已处理的事件会自动跳过，不会重复计算

## ❓ 常见问题

### Q1: 程序运行很慢怎么办？
**A**: 
- 检查是否使用了多进程（查看日志中的进程数）
- 减少保留给系统的 CPU 核心数
- 确保数据已导入 SQLite 数据库（而不是直接读取 CSV）

### Q2: 某些事件计算失败，显示"数据获取失败"？
**A**: 
- 检查股票代码是否正确
- 确认股票数据的时间范围是否覆盖事件日期
- 检查指数数据是否完整
- 查看日志文件获取详细错误信息

### Q3: 如何修改事件窗口和估计窗口？
**A**: 
- 在 `process_event_wrapper` 函数中修改 `EventStudy` 类的初始化参数
- 注意：修改后需要重新运行程序，已处理的事件不会自动更新

### Q4: 如何更换市场指数？
**A**: 
- 在 `process_event_wrapper` 函数中修改 `custom_index` 参数
- 或者设置为 `None` 以自动匹配市场指数

### Q5: 程序中断后如何继续？
**A**: 
- 直接重新运行程序即可
- 程序会自动检测已处理的事件并跳过
- 已处理的结果会保留在 CSV 文件中

### Q6: 如何查看详细的处理日志？
**A**: 
- 查看 `Event study_code/Event study program/event_study.log` 文件
- 日志级别设置为 INFO，包含详细的处理信息

### Q7: 统计检验的 p 值如何解释？
**A**: 
- $p$ 值 < 0.05：在 5% 的显著性水平下拒绝原假设（认为异常收益率显著不为 0）
- $p$ 值 < 0.01：在 1% 的显著性水平下拒绝原假设（高度显著）
- $p$ 值 >= 0.05：不能拒绝原假设（异常收益率不显著）

## 📄 License

MIT License

---

**作者**：Event Study Analysis System  
**版本**：1.0  
**最后更新**：2025-11-25

如有问题或建议，欢迎提交 Issue 或 Pull Request。
