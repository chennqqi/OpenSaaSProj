# OpenSaaSProj
移动客户端SaaS统计后台计算框架

# 模块说明
## DBClient：数据库交互模块，mongoDB、Mysql

## SaaSSMode：数据模型定义模块
## LogController：日志->数据模型
## SaaSStore：数据模型存储模块
## SaaSConfig：项目配置文件
## SaaSTools：通用功能模块
## Tongji
###  /AnalysisCommon：通用指标计算模块，包含：
                                    每日汇总
                                    每周汇总
                                    事件榜单（天/小时）
                                    页面榜单（天/小时）
                                    启动分布
                                    使用时长分布
                                    地域分布
                                    活跃度
                                    系统版本
                                    用户机型
                                    页面来源（H5）...
                                    
 ## /StoreCommon：通用指标入库模块；
 ## /Mode：通用指标模型；
 ## /Customize*：订制化统计指标开发；
  
## Transform：日志格式转化模块
