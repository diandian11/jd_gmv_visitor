# 京东指数转换开发说明
> 由于京东指数5月1后改版，导致原先指数转化模型不能正常使用，对现在主存在问题及解决思路做如下梳理和说明。
1. 不同账号所查看指数的数量级不同；
2. 不同指数转化的模型非统一性。

## 1. 解决思路及方案：
### 1.1 继续优化之前金额指数转化的模型
### 1.2 开发新指标指数转化模型
        访客指数 > 访客数
        点击搜索指数 > 点击搜索树

## 2. 模型的开发尽量具有自动智能性  
### 1.1 不同店铺、不同指标直接输入数据能够训练到一定的模型，能够自动分段，模型选择，误差控制，保证增函数
### 1.2 该模型可用于以后版本的迭代，只需输入数据即可以得到解决。
```sql

```
![tupian](https://pic2.zhimg.com/50/v2-700affa71d5cf9fdcc4ca0d163c6e208_hd.jpg)