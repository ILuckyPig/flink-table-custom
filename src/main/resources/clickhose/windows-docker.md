**docker for win环境下只能使用docker bridge network，该模式下不能通过container ip来直接访问容器内部，只能通过端口映射方式访问。**

解决方法：
1. 在<设备管理器>中创建一个回环网络适配器
2. 将网络适配器ip设置为container ip，子网掩码设置255.255.255.255
3. 如果是由多个container组成集群环境，可以在高级里面设置多个container ip
4. 这样就可以直接通过ip访问容器内部了