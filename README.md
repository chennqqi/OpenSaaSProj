# OpenSaaSProj
移动客户端SaaS统计后台计算框架

# 模块说明
 DBClient：数据库交互模块，mongoDB、Mysql</br>
 SaaSSMode：数据模型定义模块</br>
 LogController：日志->数据模型</br>
 SaaSStore：数据模型存储模块</br>
 SaaSConfig：项目配置文件</br>
 SaaSTools：通用功能模块</br>
 Tongji</br>
&emsp;&emsp;  /AnalysisCommon：通用指标计算模块，</br>&emsp;&emsp;包含：</br>
&emsp;&emsp;&emsp;每日汇总</br>
&emsp;&emsp;&emsp;每周汇总</br>
&emsp;&emsp;&emsp;事件榜单（天/小时）</br>
&emsp;&emsp;&emsp;页面榜单（天/小时）</br>
&emsp;&emsp;&emsp;启动分布</br>
&emsp;&emsp;&emsp;使用时长分布</br>
&emsp;&emsp;&emsp;地域分布</br>
&emsp;&emsp;&emsp;活跃度</br>
&emsp;&emsp;&emsp;留存分析</br>
&emsp;&emsp;&emsp;系统版本</br>
&emsp;&emsp;&emsp;用户机型</br>
&emsp;&emsp;&emsp;页面来源（H5）...</br>                             
&emsp;&emsp;/StoreCommon：通用指标入库模块；</br>
&emsp;&emsp;/Mode：通用指标模型；</br>
&emsp;&emsp;/Customize*：订制化统计指标开发；</br>
Transform：日志格式转化模块
