页面索引缓存了每一页的空闲空间, GoDB将一页的空间划分为了40个区间, 在启动的时候遍历所有的页面信息去获取页面的空闲空间, 然后根据空闲空间的大小安排到这40个区间中, insert在请求一个页的时候会将所需要的空间向上取整, 映射到这40个区间的值当中, 然后随机取出这个区间中的任何一页

从page index中选择的页会直接从index缓存中移除, 这意味着同一个页面是不允许并发写的, 需要在写完后重新插入到page index中. page_index的初始化并不是page index这个类自己做的, 它只是创建出了40块区间, 没有填充数据, 真正的初始化是在data manager中进行的

data item是DM层向上层提供的数据抽象, dataitem中保存的数据结构为: [ValidFlag][Datasize][Data], 其中validflag占1个字节, 标识该数据是否有效, 要删除数据只需要将其设置为0, datasize占2字节, 标识了这条data的长度, 上层模块在获取到dataitem后就可以使用data()方法获取到数据

上层模块在对dataitem进行修改时, 需要遵循一定的流程: 修改前调用before()方法, 撤销修改调用unBefore()方法, 修改完成后调用after()方法. 这个流程是为了保存前相数据, 并及时写日志

data manager是DM层直接对外提供方法的类, 同时也实现成dataitem对象的缓存. dataitem的key就是uid, 由页号和页内偏移组成的一个8字节的无符号整数