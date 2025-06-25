对应MYDB的tm/TransactionManager.java和TransactionManagerImpl.java

##

###

事务有3种状态，active, committed和aborted

GoDB中的事务管理通过维护XID文件实现，xid从1开始自增，xid = 0是一个超级事务，用于让一些操作在不申请事务的情况下进行，xid = 0这个超级事务的状态永远是committed

因为xid从1开始自增，所以只需要用1个整数就可以记录这一个XID文件管理了多少个事务，这个整数在GoDB中称为XID的文件头，是一个8位的整数(int64)。事务状态是确定的3种，所以便使用最小的单位字节来进行存储，即每个事务的状态在XID文件中占1字节

既然xid是自增的，那么每个事务的状态便可以依次写入XID文件，xid = n的事务的状态便保存在XID文件的第(xid - 1) + 8个字节处(从第0个字节开始)，xid - 1的原因是xid = 0这个超级事务的状态不需要被记录

至此，根据以上的内容，我们便可以定义出代码中的7个常量

###

定义了本代码中所需的一些error，目前先写在这里，待后期迁移至单独的文件

###

定义了TransactionManager接口，只包含了需要对外暴露的函数

分别是：

- Begin()开启事务
- Commit()提交事务
- Abort()终止事务
- IsActive()检验事务是否是Active状态
- IsCommitted()检验事务是否是Committed状态
- IsAborted()检验事务是否是Aborted状态
- Close()关闭一个事务

然后定义了一个不对外暴露的结构体transactionManager，分别代表当前操作的文件、XID的头文件和互斥锁

### 

Create函数是一个对外暴露的函数，通过传入一个路径，然后在该路径创建一个叫做.xid的文件作为XID文件，返回值为TransactionManager接口和错误信息error

Golang没有try-catch语法，在Go中类似的实现是返回错误信息

创建一个新的事务管理器首先要检查传入的路径是否已经存在XID文件，如果存在就立刻返回对应的错误信息

接下来创建XID文件并设置读写权限，如果创建文件失败则返回对应的错误信息

然后写入一个空的XID文件头，如果写入失败则返回对应的错误信息

再将写入内容立刻写入磁盘保存，如果刷入失败则返回对应的错误信息

最后返回一个transactionManager结构的地址，由于Golang的语言特性，没有构造函数，所以直接对结构体的元素进行赋值后返回整个结构体的地址，并且由于这里是创建新的XID文件，所以需要对xid文件头进行赋值

###

Open函数也是一个对外暴露的函数，通过传入一个路径，打开此路径下的事务管理器，即XID文件

首先要检查文件是否存在，如果不存在则返回对应的错误信息

然后打开文件，如果打开失败则返回对应的错误信息

可以打开文件后，先创建一个transactionManager的指针，其中文件就是当前的XID文件，再调用checkXIDCounter()函数检验XID文件是否有效，若XID文件不完整则返回对应的错误信息

最后返回transactionManager结构的指针

### 

checkXIDCounter函数是一个不对外暴露的函数，传入一个transactionManager类型的指针，返回一个错误信息

首先获取XID文件的真实文件大小fileLen

如果fileLen小于文件头的长度(即8个字节)，那么XID文件肯定是损坏的，便立刻返回对应的错误信息

然后再获取文件头，即xidCounter，读取该XID文件管理了多少个事务

再通过binary库自带的函数将二进制数据转换为uint64类型的整数

根据我们对XID文件的设计，一个完整的XID文件应该有xidCounter + 8个字节，所以直接将真实的文件大小与这个预期值进行比对，如果不相等则XID文件是损坏的，返回错误信息

### 

getXidPosition函数是一个不对外暴露的函数，传入一个xid，返回该xid对应的事务在XID文件中的位置

直接根据XID文件的设计计算出xid对应事务的位置即可

### 

updateXID也是一个不对外暴露的函数，传入所需要修改状态的事务和需要修改成为的状态，无返回值

首先调用封装好的getXidPosition计算xid对应事务所在XID文件中的具体位置

然后将要修改成为的状态(status)写入，最后调用Sync()将数据刷入磁盘

### 

incrXIDCounter将XID计数器加1并更新文件头的值，无入参和出参

将事务管理器所维护的xidCounter加1，然后调用binary的PutUint64方法将其转换为字节

再将修改好的文件头写入到XID文件中，并将数据刷入磁盘

### 

Begin函数是一个对外暴露的函数，开启一个新事务

首先对transactionManager进行加锁，并使用defer关键字(类似finally)，确保会释放锁

然后将xidCounter加1，并更新事务状态，但是这里不手动写入XID文件，而是调用写好的updateXID函数和incrXIDCounter函数，最后返回新的xid

### 

Commit和Abort函数只对已经存在的事务进行状态修改，所以只需要判断这个事务是否是超级事务(xid == 0)，然后调用updateXID对普通事务的状态进行修改即可

###

checkXID是一个不对外暴露的函数，用于检查一个事务的状态是否为预期的状态，传入xid和预期的状态status

根据传入xid调用getXidPosition计算该事务的位置，然后读取这个事务的状态并对其与预期的状态进行比对，返回bool结果

### 

IsActive, IsCommitted和IsAborted函数都是对外暴露的，调用checkXID函数检查事务是否为active, committed和aborted状态，其中会检查一下事务是否为超级事务，如果是的话可以直接返回，不需要调用，节约资源

### 

Close函数是一个对外暴露的函数，用于关闭事务管理器