# 3.6 Scala高级语法

### 3.6.1 高阶函数

高阶函数是指使用其他函数作为参数、或者返回一个函数作为结果的函数。因为在Scala中函数使用得最多，该术语可能会引起混淆，对于将函数作为参数或返回函数的方法和函数，我们将其定义为高阶函数。从计算机科学的角度来看，函数可以具有多种形式，例如一阶函数、高阶函数或纯函数。从数学的角度来看也是如此，使用高阶函数是可以执行以下操作之一：

（1）将一个或多个函数作为参数来执行某些操作

（2）将一个函数返回作为结果

除高阶函数外的所有其他函数均为一阶函数。但是，从数学的角度来看高阶函数也称为运算符或函数。另一方面，如果函数的返回值仅由其输入确定则称为纯函数。在本节中，我们将简要讨论为什么以及如何在scala中使用不同的函数范式。特别地，将讨论纯函数和高阶函数。在本节还将提供使用匿名函数的简要概述，因为在使用Scala开发Spark应用程序时经常使用匿名函数。

纯函数是这样一种函数，输入输出数据流全是显式的。显式的意思是，函数与外界交换数据只有一个唯一渠道：参数到返回值，函数从函数外部接受的所有输入信息都通过参数传递到该函数内部；函数输出到函数外部的所有信息都通过返回值传递到该函数外部。如果一个函数通过隐式方式，从外界获取数据，或者向外部输出数据，那么该函数就是非纯函数。隐式的意思是，函数通过参数和返回值以外的渠道，和外界进行数据交换，比如读取全局变量和修改全局变量都叫作以隐式的方式和外界进行数据交换；比如利用输入输出系统函数库读取配置文件，或者输出到文件，打印到屏幕，都叫做隐式的方式和外界进行数据交换。我们看一下纯函数与非纯函数的例子：

  - > 纯函数

```scala
scala> def add(a:Int,b:Int) = a + b
add: (a: Int, b: Int)Int
scala> var a = 1
a: Int = 1
```

  - > 非纯函数

```scala
scala> def addA(b:Int) = a + b
addA: (b: Int)Int
scala>
scala> def add(a:Int,b:Int) = {
| println(s"a:$a b:$b")
| a + b
| }
add: (a: Int, b: Int)Int
scala> def randInt() = Random.nextInt()
<console>:30: error: not found: value Random
def randInt() = Random.nextInt()
^
scala> import scala.util.Random
import scala.util.Random
scala> def randInt() = Random.nextInt()
randInt: ()Int
```

那么纯函数有什么好处？纯函数通常比其他函数代码量要少，尽管这也取决于其他因素，例如编程语言。并且由于看起来像数学函数，因此更容易被我们解释和理解。纯函数是函数式编程的核心功能，也是一种最佳实践，我们需要使用纯函数构建应用程序的核心部分。在编程领域中，函数是一段通过名称调用的代码，可以传递数据作为参数以对其进行操作并可以返回数据
传递给函数的所有数据都会显式传递。另一方面，方法也是一段通过名称调用的代码。但是，一个方法始终与一个对象相关联，作为对象的一个属性。在大多数情况下方法与功能是相同的，除了两个主要区别：

（1）方法被隐式地传递给被调用的对象

（2）方法可以对类中包含的数据进行操作

有时在的代码中，我们不想在使用函数之前先定义一个函数，也许是因为只需要在一个地方被使用，而不需要通过函数名在其他地方调用。在函数式编程中，有一类函数非常适合这种情况，称为匿名函数。Scala中的匿名函数是没有方法名，也不用def定义函数。一般匿名函数都是一个表达式，因此匿名函数非常适合替换那些只用一次且任务简单的常规函数，所以使得我们的代码变得更简洁了。匿名函数的语法很简单，箭头“=\>”左边是参数列表，右边是函数体。定义匿名函数的语法为:

(param1, param2) =\> \[expression\]

下面的表达式就定义了一个接受Int类型输入参数的匿名函数:

```scala
scala> var inc = (x:Int) => x+1
inc: Int => Int = <function1>
```

上述定义的匿名函数，其实是下面这个常规函数的简写：

def add(x:Int):Int {

return x+1;

}

以上范例中的 inc被定义一个值，使用方式如下：

```scala
scala> var x = inc(7)-1
x: Int = 7
```

同样我们可以在匿名函数中定义多个参数：

```scala
scala> var mul = (x: Int, y: Int) => x*y
mul: (Int, Int) => Int = <function2>
scala> println(mul(3, 4))
12
```

我们也可以不给匿名函数设置参数，如下所示：

```scala
scala> var userDir = () => { System.getProperty("user.dir") }
userDir: () => String = <function0>
scala> println( userDir() )
/root
```

下划线“\_”可用作是匿名函数参数的占位符，但对于每一个参数，只能用下划线占位一次。在 Scala 中，\_ \* \_
表示匿名函数接受2个参数，函数返回值是两个参数的乘积。例如下列 Scala 代码中的
print(\_) 相当于 x =\> print(x)：

```scala
scala> List(1, 2, 3, 4, 5).foreach(print(_))
12345
scala> List(1, 2, 3, 4, 5).reduceLeft(_ + _)
res52: Int = 15
```

最常见的一个例子是Scala集合类的高阶函数map()：

```scala
scala> val salaries = Seq(20000, 70000, 40000)
salaries: Seq[Int] = List(20000, 70000, 40000)
scala> val doubleSalary = (x: Int) => x * 2
doubleSalary: Int => Int = <function1>
scala> val newSalaries = salaries.map(doubleSalary)
newSalaries: Seq[Int] = List(40000, 140000, 80000)
```

函数doubleSalary()有一个整型参数x，返回x \*
2。一般来说，在“=\>”左边的元组是函数的参数列表，而右边表达式的值则为函数的返回值。在map()中调用函数doubleSalary()将其应用到列表salaries中的每一个元素上。为了简化压缩代码，我们可以使用匿名函数，直接作为参数传递给map()。注意在上述示例中x没有被显式声明为Int类型，这是因为编译器能够根据map函数期望的类型推断出x的类型。对于上述代码，一种更惯用的写法为：

```scala
scala> val newSalaries = salaries.map(_ * 2)
newSalaries: Seq[Int] = List(40000, 140000, 80000)
```

既然Scala编译器已经知道了参数的类型，我们可以只给出函数的右半部分，不过需要使用“\_”代替参数名。我们同样可以传入一个对象方法作为高阶函数的参数，这是因为Scala编译器会将方法强制转换为一个函数。

```scala
scala> case class WeeklyWeatherForecast(temperatures: Seq[Double]) {
| private def convertCtoF(temp: Double) = temp * 1.8 + 32
| def forecastInFahrenheit: Seq[Double] =
temperatures.map(convertCtoF)
| }
defined class WeeklyWeatherForecast
```

在这个例子中，方法convertCtoF()被传入forecastInFahrenheit()。这是可以的，因为编译器强制将方法convertCtoF转成了函数x
=\> convertCtoF(x) ，x是编译器生成的变量名，保证在其作用域是唯一的。有一些情况我们希望生成一个函数，比如：

```scala
scala> def urlBuilder(ssl: Boolean, domainName: String): (String,
String) => String = {
| val schema = if (ssl) "https://" else "http://"
| (endpoint: String, query: String) =>
s"$schema$domainName/$endpoint?$query"
| }
urlBuilder: (ssl: Boolean, domainName: String)(String, String) =>
String
scala> val domainName = "www.example.com"
domainName: String = www.example.com
scala> def getURL = urlBuilder(ssl=true, domainName)
getURL: (String, String) => String
scala> val endpoint = "users"
endpoint: String = users
scala> val query = "id=1"
query: String = id=1
scala> val url = getURL(endpoint, query) //
"https://www.example.com/users?id=1": String
url: String = <https://www.example.com/users?id=1>
```

urlBuilder的返回类型是(String, String) =\>
String，这意味着返回的是匿名函数，其有两个String参数，返回一个String。

在scala相关的教程与参考文档里，经常会看到柯里化函数这个词。但是对于具体什么是柯里化函数，柯里化函数又有什么作用，其实可能很多同学都会有些疑惑，首先看两个简单的函数：

```scala
scala> def add(x: Int, y: Int) = x + y
add: (x: Int, y: Int)Int
scala> add(2, 1)
res54: Int = 3
scala> def addCurry(x: Int)(y: Int) = x + y
addCurry: (x: Int)(y: Int)Int
scala> addCurry(2)(1)
res55: Int = 3
```

以上两个函数实现的都是两个整数相加的功能。对于add函数，调用方式为add(1,2)。对于addCurry函数，调用的方式为addCurry(1)(2)，这种方式就叫做柯里化。addCurry
(1)(2) 实际上是依次调用两个普通函数，第一次调用使用一个参数
x，返回一个函数类型的值，第二次使用参数y调用这个函数类型的值，那么这个函数是什么意思呢？接收一个x为参数，返回一个匿名函数，该匿名函数的定义是：接收一个Int型参数y，函数体为x+y。现在我们来对这个函数进行调用：

```scala
scala> def add(x:Int)=(y:Int)=>x+y
add: (x: Int)Int => Int
scala> val result = add(1)
result: Int => Int = <function1>
scala> val sum = result(2)
sum: Int = 3
```

例子中返回一个result，那result的值应该是一个匿名函数：(y:Int)=\>1+y，所以为了得到结果，我们继续调用result，最后打印出来的结果就是3。

柯里函数最大的意义在于把多个参数的函数等价转化成多个单参数函数的级联，这样所有的函数就都统一方便做lambda演算。在Scala中，函数的柯里化对类型推演也有帮助，Scala的类型推演是局部的，在同一个参数列表中后面的参数不能借助前面的参数类型进行推演。通过柯里化函数后，后面的参数可以借助前面的参数参数类型进行推演。两个参数的函数可以拆分，同理三个参数的函数同样也可以柯里化：

```scala
scala> def add(x:Int)(y:Int)(z:Int)= x + y + z
add: (x: Int)(y: Int)(z: Int)Int
scala> add(10)(10)(10)
res19: Int = 30
```

简单看一个柯里化函数foldLeft()的定义：

  - def foldLeft\[B\](z: B)(op: (B, A) ⇒ B): B

这个函数在集合中很有用，其中B表示泛型，第一个(z: B)传递一个B类型的参数z，第二个(op: (B, A) ⇒
B)表示op参数表示为一个匿名函数，foldLeft()函数返回一个B类型的参数。foldLeft()函数将包含两个参数的函数op应用于初始值z和该集合的所有元素上，从左到右。
下面显示的是其用法示例。从初始值0开始，此处foldLeft将函数(m, n) =\> m +
n作为参数op，应用于列表array中的每个元素和先前的累加值0：

```scala
scala> val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
numbers: List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
scala> numbers.foldLeft(0)((m, n) => m + n)
res56: Int = 55
scala> numbers.foldLeft(0)(_ + _)
res57: Int = 55
```

请注意，如果使用多个参数列表的柯里函数，能够利用Scala类型推断使代码更简洁。下划线在scala中很有用，比如在初始化某一个变量的时候了下划线代表的是这个变量的默认值。在函数中下划线代表的是占位符，用来表示一个函数的参数，其名字和类型都会被隐式的指定了，当然如果Scala无法判断下划线代表的类型，那么就可能要报错了。另外，Scala还定义了foldLeft()另外一种替换方式：

  - def/:\[B\](z: B)(op: (B, A) ⇒ B): B

所以上面的代码也可以写为：

```scala
scala> val res = (0/:numbers) ((m, n) => m + n)
res: Int = 55
```

### 3.6.2 泛型类

泛型类指可以接受类型参数的类。泛型类在集合类中被广泛使用。泛型类使用方括号\[\] 来接受类型参数。一个惯例是使用字母 A
作为参数标识符，当然我们可以使用任何参数名称。

```scala
scala> class Stack[A] {
| private var elements: List[A] = Nil
| def push(x: A) { elements = x :: elements }
| def peek: A = elements.head
| def pop(): A = {
| val currentTop = peek
| elements = elements.tail
| currentTop
| }
| }
defined class Stack
```

上面的 Stack
类的定义中接受类型参数A，这意味着其内部的列表elements只能够存储类型A的元素，方法push()只接受类型A的实例对象作为参数，将x添加到elements前面然后重新分配给一个新的列表。要使用一个泛型类，需要将一个具体类型放到方括号中来代替A。

```scala
scala> val stack = new Stack[Int]
stack: Stack[Int] = Stack@b0b2fcc
scala> stack.push(1)
scala> stack.push(2)
scala> stack.pop
res62: Int = 2
scala> stack.pop
res63: Int = 1
```

上面的实例对象 stack 只能接受整型值，然而如果类型参数有子类型，子类型可以被传入：

```scala
scala> class Fruit
defined class Fruit
scala> class Apple extends Fruit
defined class Apple
scala> class Banana extends Fruit
defined class Banana
scala> val stack = new Stack[Fruit]
stack: Stack[Fruit] = Stack@2d11ac93
scala> val apple = new Apple
apple: Apple = Apple@5e36c9e6
scala> val banana = new Banana
banana: Banana = Banana@115e07de
scala> stack.push(apple)
scala> stack.push(banana)
scala> stack.pop
res66: Fruit = Banana@115e07de
scala> stack.pop
res67: Fruit = Apple@5e36c9e6
```

类 Apple 和类 Banana 都继承自类 Fruit，所以我们可以把实例对象 apple 和 banana
压入stack中。泛型类型的子类是不可变，这表示如果我们有一个Char类型的栈
Stack\[Char\]，那它不能被用作一个Int的栈 Stack\[Int\]，否则就是不安全的。只有当类型 B = A 时，
Stack\[A\] 是 Stack\[B\] 的子类成立。Scala 提供了一种类型参数注释机制用以控制泛型类型的子类行为。

型变是复杂类型的子类型关系与其组件类型的子类型关系的相关性。Scala支持泛型类
的类型参数的型变注释，允许它们是协变的，逆变的，或在没有使用注释的情况下是不变的。在类型系统中使用型变允许我们在复杂类型之间建立直观的连接，而缺乏型变则会限制类抽象的重用性。

class Foo\[+A\] // A covariant class

class Bar\[-A\] // A contravariant class

class Baz\[A\] // An invariant class

协变

使用注释 +A，可以使一个泛型类的类型参数 A 成为协变。 对于某些类 class List\[+A\]，使 A 成为协变意味着对于两种类型 A
和 B，如果 A 是 B 的子类型，那么 List\[A\] 就是 List\[B\] 的子类型。
这允许我们使用泛型来创建非常有用和直观的子类型关系。

考虑以下简单的类结构：

abstract class Animal {

def name: String

}

case class Cat(name: String) extends Animal

case class Dog(name: String) extends Animal

类型 Cat 和 Dog 都是 Animal 的子类型。 Scala 标准库有一个通用的不可变的类 sealed abstract class
List\[+A\]，其中类型参数 A 是协变的。 这意味着 List\[Cat\] 是 List\[Animal\]，List\[Dog\]
也是 List\[Animal\]。 直观地说，猫的列表和狗的列表都是动物的列表是合理的，你应该能够用它们中的任何一个替换
List\[Animal\]。

在下例中，方法 printAnimalNames 将接受动物列表作为参数，并且逐行打印出它们的名称。 如果 List\[A\]
不是协变的，最后两个方法调用将不能编译，这将严重限制 printAnimalNames 方法的适用性。

object CovarianceTest extends App {

def printAnimalNames(animals: List\[Animal\]): Unit = {

animals.foreach { animal =\>

println(animal.name)

}

}

val cats: List\[Cat\] = List(Cat("Whiskers"), Cat("Tom"))

val dogs: List\[Dog\] = List(Dog("Fido"), Dog("Rex"))

printAnimalNames(cats)

// Whiskers

// Tom

printAnimalNames(dogs)

// Fido

// Rex

}

逆变

通过使用注释 -A，可以使一个泛型类的类型参数 A 成为逆变。 与协变类似，这会在类及其类型参数之间创建一个子类型关系，但其作用与协变完全相反。
也就是说，对于某个类 class Writer\[-A\] ，使 A 逆变意味着对于两种类型 A 和 B，如果 A 是 B 的子类型，那么
Writer\[B\] 是 Writer\[A\] 的子类型。

考虑在下例中使用上面定义的类 Cat，Dog 和 Animal ：

abstract class Printer\[-A\] {

def print(value: A): Unit

}

这里 Printer\[A\] 是一个简单的类，用来打印出某种类型的 A。 让我们定义一些特定的子类：

class AnimalPrinter extends Printer\[Animal\] {

def print(animal: Animal): Unit =

println("The animal's name is: " + animal.name)

}

class CatPrinter extends Printer\[Cat\] {

def print(cat: Cat): Unit =

println("The cat's name is: " + cat.name)

}

如果 Printer\[Cat\] 知道如何在控制台打印出任意 Cat，并且 Printer\[Animal\] 知道如何在控制台打印出任意
Animal，那么 Printer\[Animal\] 也应该知道如何打印出 Cat 就是合理的。 反向关系不适用，因为
Printer\[Cat\] 并不知道如何在控制台打印出任意 Animal。 因此，如果我们愿意，我们应该能够用
Printer\[Animal\] 替换 Printer\[Cat\]，而使 Printer\[A\] 逆变允许我们做到这一点。

object ContravarianceTest extends App {

val myCat: Cat = Cat("Boots")

def printMyCat(printer: Printer\[Cat\]): Unit = {

printer.print(myCat)

}

val catPrinter: Printer\[Cat\] = new CatPrinter

val animalPrinter: Printer\[Animal\] = new AnimalPrinter

printMyCat(catPrinter)

printMyCat(animalPrinter)

}

这个程序的输出如下：

The cat's name is: Boots

The animal's name is: Boots

不变

默认情况下，Scala中的泛型类是不变的。 这意味着它们既不是协变的也不是逆变的。 在下例中，类 Container 是不变的。
Container\[Cat\] 不是 Container\[Animal\]，反之亦然。

class Container\[A\](value: A) {

private var \_value: A = value

def getValue: A = \_value

def setValue(value: A): Unit = {

\_value = value

}

}

可能看起来一个 Container\[Cat\] 自然也应该是一个
Container\[Animal\]，但允许一个可变的泛型类成为协变并不安全。
在这个例子中，Container 是不变的非常重要。 假设 Container 实际上是协变的，下面的情况可能会发生：

val catContainer: Container\[Cat\] = new Container(Cat("Felix"))

val animalContainer: Container\[Animal\] = catContainer

animalContainer.setValue(Dog("Spot"))

val cat: Cat = catContainer.getValue // 糟糕，我们最终会将一只狗作为值分配给一只猫

幸运的是，编译器在此之前就会阻止我们。

其他例子

另一个可以帮助理解型变的例子是 Scala 标准库中的 trait Function1\[-T, +R\]。 Function1
表示具有一个参数的函数，其中第一个类型参数 T 表示参数类型，第二个类型参数 R 表示返回类型。
Function1 在其参数类型上是逆变的，并且在其返回类型上是协变的。 对于这个例子，我们将使用文字符号 A =\> B 来表示
Function1\[A, B\]。

假设前面使用过的类似 Cat，Dog，Animal 的继承关系，加上以下内容：

abstract class SmallAnimal extends Animal

case class Mouse(name: String) extends SmallAnimal

假设我们正在处理接受动物类型的函数，并返回他们的食物类型。 如果我们想要一个 Cat =\>
SmallAnimal（因为猫吃小动物），但是给它一个 Animal =\>
Mouse，我们的程序仍然可以工作。 直观地看，一个 Animal =\> Mouse 的函数仍然会接受一个 Cat 作为参数，因为 Cat
即是一个 Animal，并且这个函数返回一个 Mouse，也是一个 SmallAnimal。
既然我们可以安全地，隐式地用后者代替前者，我们可以说
Animal =\> Mouse 是 Cat =\> SmallAnimal 的子类型。

与其他语言的比较

某些与 Scala 类似的语言以不同的方式支持型变。 例如，Scala 中的型变注释与 C\#
中的非常相似，在定义类抽象时添加型变注释（声明点型变）。
但是在Java中，当类抽象被使用时（使用点型变），才会给出型变注释。

### 3.6.3 隐式转换

Scala
的隐式转换定义了一套查找机制，当编译器发现代码出现类型转换时，编译器试图去寻找一种隐式的转换方法，从而使得编译器能够自我修复完成编译。在
Scala
语言当中，隐式转换是一项强大的程序语言功能，它不仅能够简化程序设计，也能够使程序具有很强的灵活性，可以在不修改原有的类的基础上，对类的功能进行扩展。比如，在
Spark 源码中，经常会发现 RDD 这个类没有 reduceByKey()、groupByKey() 等方法定义，但是却可以在 RDD
上调用这些方法。这就是 Scala 隐式转换导致的，如果需要在RDD上调用这些函数，RDD必须是RDD\[(K,
V)\]类型，即键值对类型。我们可以参考Spark源码文件，在RDD这个对象上定义了一个rddToPairRDDFunctions隐式转换。

/\*\*

\*/

object RDD {

private\[spark\] val CHECKPOINT\_ALL\_MARKED\_ANCESTORS =

"spark.checkpoint.checkpointAllMarkedAncestors"

// The following implicit functions were in SparkContext before 1.3 and
users had to

// \`import SparkContext.\_\` to enable them. Now we move them here to
make the compiler find

// them automatically. However, we still keep the old functions in
SparkContext for backward

// compatibility and forward to the following functions directly.

implicit def rddToPairRDDFunctions\[K, V\](rdd: RDD\[(K, V)\])

(implicit kt: ClassTag\[K\], vt: ClassTag\[V\], ord: Ordering\[K\] =
null): PairRDDFunctions\[K, V\] = {

new PairRDDFunctions(rdd)

}

rddToPairRDDFunction为隐式转换函数，即将RDD\[(K,
V)\]类型转换为PairRDDFunctions对象，从而可以在原始的RDD对象上调用reduceByKey()之类的方法。rddToPairRDDFunction隐式函数位于1.3之前的SparkContext中，我们必须使用import
SparkContext.\_以启用它们，现在将它们移出以使编译器自动找到它们。但是，我们仍将旧功能保留在SparkContext中以实现向后兼容，并直接转发至以下功能。隐式转换是Scala的一大特性，如果对其不是很了解，在阅读Spark代码时候就会感到很困难。上面对Spark中的隐式类型转换做了分析，现在从Scala语法的角度对隐式转换进行总结。从一个简单例子出发，我们定义一个函数接受一个字符串参数，并进行输出：

```scala
scala> def func(msg:String) = println(msg)
func: (msg: String)Unit
scala> func("11")
11
scala> func(11)
<console>:34: error: type mismatch;
found : Int(11)
required: String
func(11)
^
```

这个函数在func("11")调用时候正常，但是在执行func(11)或func(1.1)时候就会报error: type
mismatch的错误，对于这个问题有多种方式解决，其中可以包括：

（1）针对特定的参数类型，重载多个func函数，但是需要定义多个函数；

（2）msg参数使用超类型，比如使用AnyVal或Any（Any是所有类型的超类。Any具有两个直接子类：AnyVal和AnyRef），但是需要在函数中针对特定的逻辑做类型转化，从而进一步处理。

这两个方式使用了面向对象编程的思路，虽然都可以解决该问题，但是不够简洁。在Scala中，针对类型转换提供了特有的隐式转化功能。我们通过一个函数实现隐式转化，这个函数可以根据一个变量在需要的时候调用进行类型转换。针对上面的例子，我们可以定义intToString函数：

```scala
scala> implicit def intToString(i:Int)=i.toString
warning: there was one feature warning; re-run with -feature for details
intToString: (i: Int)String
scala> func(11)
11
scala> implicit def intToStr(i:Int)=i.toString
warning: there was one feature warning; re-run with -feature for details
intToStr: (i: Int)String
scala> func(11)
<console>:38: error: type mismatch;
found : Int(11)
required: String
Note that implicit conversions are not applicable because they are
ambiguous:
both method intToString of type (i: Int)String
and method intToStr of type (i: Int)String
are possible conversion functions from Int(11) to String
func(11)
^
```

此时在调用func(11)的时候，Scala编译器会自动对参数11进行intToString函数的调用，从而通过Scala的隐形转换实现func函数对字符串参数类型的支持。上例中，隐式转换依据的条件是输入参数类型（Int）和目标参数类型（String）的匹配，至于函数名称并不重要。如果取为intToString可以直观的表示，如果使用int2str也是一样的。隐式转换只关心类型，所以如果同时定义两个类型相同的隐式转换函数，但是函数名称不同时，这个时候函数调用过程中如果需要进行类型转换，就会报二义性的错误，即不知道使用哪个隐式转换函数进行转换。
