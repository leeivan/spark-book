# 2.5 Scala编程

&emsp;&emsp;Scala和Spark一直是很自然的组合。Scala既支持面向对象编程（Object-Oriented Programming），也支持函数式编程（Functional Programming），因此既适合组织工程代码，也适合表达数据转换逻辑。面向对象编程强调类、对象、封装与复用，便于构建模块化系统；函数式编程强调不可变数据、纯函数和表达式求值，能够更自然地描述RDD、DataFrame这类分布式数据处理流程。两种风格结合起来，正好契合Spark“工程代码 + 数据转换”的开发场景。

&emsp;&emsp;很多数据分析人员更熟悉Python或R，这也是今天最常见的Spark入口语言。不过，Scala仍然值得学习，因为它更贴近Spark的原生实现。历史上，大量Spark书籍、源码示例和底层API讨论都以Scala为主。使用Scala学习Spark，有几个直接好处：更容易理解Spark的执行模型；更方便对照官方源码和示例；在需要访问新特性或底层能力时，通常也能更早接触到完整API。

&emsp;&emsp;无论是使用Scala调用Spark SQL、Structured Streaming、spark.ml和GraphX等组件，还是在本地或集群环境中开发Spark应用，Scala都能帮助我们更直接地理解Spark的抽象层和运行机制。这也是本节安排Scala基础内容的原因：不是为了单独学习一门语言，而是为了让后续的Spark示例更容易读懂、改写和扩展。

&emsp;&emsp;在本节中，我们将讨论Scala中基本的面向对象功能，将涵盖的主题包括：Scala中的变量；Scala中的方法，类和对象；包和包对象；特性和特征线性化。然后，我们将讨论模式匹配，这是来自功能编程概念的功能。此外，我们将讨论Scala中的一些内置概念，例如隐式和泛型。最后，我们将讨论将Scala应用程序构建到jar中所需的一些广泛使用的构建工具。

### 2.5.1 面向对象编程

&emsp;&emsp;Scala
&emsp;&emsp;REPL是命令行解释器，可以将其用作测试Scala代码的环境。要启动REPL会话，只需在本教程提供的虚拟环境的命令中键入scala，将看到以下内容：

&emsp;&emsp;root@bb8bf6efccc9:\~\# scala

&emsp;&emsp;Welcome to Scala 2.13.16 (OpenJDK 64-Bit Server VM, Java 17).

&emsp;&emsp;Type in expressions for evaluation. Or try :help.

```scala
scala>
```

&emsp;&emsp;因为REPL是命令行解释器，所以需要键入代码然后回车执行就可以看到结果。进入REPL后，可以键入Scala表达式以查看其工作方式：

```scala
scala> val x = 1
x: Int = 1
scala> val y = x + 1
y: Int = 2
```

&emsp;&emsp;如这些示例所示，只需在REPL内键入表达式，它就会在下一行上显示每个表达式的结果。

&emsp;&emsp;scala
&emsp;&emsp;REPL会根据需要创建变量，如果不将表达式的结果分配给变量，则REPL会自动创建以res为开头的变量，第一个变量是res0，第二个变量是res1，等等：

```scala
scala> 2 + 2
res0: Int = 4
scala> 3 / 3
res1: Int = 1
```

&emsp;&emsp;这些是动态创建的实际变量名，可以在表达式中使用它们：

```scala
scala> val z = res0 + res1
z: Int = 5
```

&emsp;&emsp;上面简单介绍了scala REPL的使用。在本书中大部分的例子使用了Spark
&emsp;&emsp;Shell工具，这就是Spark提供的REPL，只是在启动工具时加载了Spark程序包，可以直接在命令上调用。这里继续使用REPL进行实验。下面是一些表达式，可以尝试看看它们如何工作：

```scala
scala> val name = "John Doe"
name: String = John Doe
scala> "hello".head
res0: Char = h
scala> "hello".tail
res1: String = ello
scala> "hello, world".take(5)
res2: String = hello
scala> println("hi")
hi
scala> 1 + 2 * 3
res4: Int = 7
scala> (1 + 2) * 3
res5: Int = 9
scala> if (2 > 1) println("greater") else println("lesser")
greater
```

&emsp;&emsp;Scala具有两种类型的变量：val类型创建一个不可变的变量，例如在Java中的final）；var创建一个可变变量。这是Scala中的变量声明：

```scala
scala> val s = "hello"
s: String = hello
scala> var i = 42
i: Int = 42
```

&emsp;&emsp;这些示例表明Scala编译器通常可以从“=”符号右侧的代码推断出变量的数据类型，所以说变量的类型可以由编译器推断的。如果愿意，还可以显式声明变量类型：

```scala
scala> val s: String = "hello"
s: String = hello
scala> var i: Int = 42
i: Int = 42
```

&emsp;&emsp;在大多数情况下，编译器不需要查看那些显式类型，但是如果您认为它们使代码更易于阅读，则可以添加它们。实际上，当使用第三方库中的方法时，特别是如果不经常使用该库或它们的方法名称不能使类型清晰时，可以帮助提示变量类型。

&emsp;&emsp;val和var之间的区别是：val使变量不变，var并使变量可变。由于val字段不能改变，因此有些人将其称为值而不是变量。当尝试重新分配val字段时，REPL显示会发生什么：

```scala
scala> val a = 'a'
a: Char = a
scala> a = 'b'
<console>:12: error: reassignment to val
a = 'b'
^
```

&emsp;&emsp;正如预期的那样，此操作失败并显示val的重新分配错误，相反我们可以重新分配var：

```scala
scala> var a = 'a'
a: Char = a
scala> a = 'b'
a: Char = b
```

&emsp;&emsp;REPL与在IDE中使用源代码并非100％相同，因此在REPL中可以做一些事情，而在编写scala应用程序中是做不到的，例如可以使用val方法在REPL中重新定义变量，如下所示：

```scala
scala> val age = 18
age: Int = 18
scala> val age = 19
age: Int = 19
```

&emsp;&emsp;而在scala应用程序代码中，不能使用val方法重新定义变量，但是可以在REPL中重新定义。Scala带有标准数字数据类型。在Scala中，所有这些数据类型都是对象，不是原始数据类型。这些示例说明如何声明基本数字类型的变量：

```scala
scala> val b: Byte = 1
b: Byte = 1
scala> val x: Int = 1
x: Int = 1
scala> val l: Long = 1
l: Long = 1
scala> val s: Short = 1
s: Short = 1
scala> val d: Double = 2.0
d: Double = 2.0
scala> val f: Float = 3.0f
f: Float = 3.0
```

&emsp;&emsp;在前四个例子中，如果没有明确指定类型，数量1将默认为Int，所以如果需要其他数据类型Byte、Long或者Short中的一种，需要显式声明的类型。带小数的数字（如2.0）将默认为双精度，因此如果需要单精度，则需要使用Float类型声明。

&emsp;&emsp;因为Int和Double是默认数字类型，所以通常在不显式声明数据类型的情况下创建它们：

```scala
scala> val i = 123
i: Int = 123
scala> val x = 1.0
x: Double = 1.0
```

&emsp;&emsp;对于大数，Scala还包括类型BigInt和BigDecimal：

```scala
scala> var b = BigInt(1234567890)
b: scala.math.BigInt = 1234567890
scala> var b = BigDecimal(123456.789)
b: scala.math.BigDecimal = 123456.789
```

&emsp;&emsp;BigInt和BigDecimal的一大优点是它们支持您习惯于使用数值类型的所有运算符。Scala还具有String和Char数据类型，通常可以使用隐式形式进行声明：

```scala
scala> val name = "Bill"
name: String = Bill
scala> val c = 'a'
c: Char = a
```

&emsp;&emsp;如上例所示，将字符串括在双引号中，将字符括在单引号中。Scala字符串具有很多不错的功能，其中一个功能是Scala具有一种类似于Ruby的方式来合并多个字符串：

```scala
scala> val firstName = "John"
firstName: String = John
scala> val mi = 'C'
mi: Char = C
scala> val lastName = "Doe"
lastName: String = Doe
```

&emsp;&emsp;可以按以下方式将它们附加在一起：

```scala
scala> val name = firstName + " " + mi + " " + lastName
name: String = John C Doe
```

&emsp;&emsp;但是，Scala提供了以下更方便的形式：

```scala
scala> val name = s"$firstName $mi $lastName"
name: String = John C Doe
```

&emsp;&emsp;这种形式创建了一种非常易读的方式来打印包含变量的字符串：

```scala
scala> println(s"Name: $firstName $mi $lastName")
Name: John C Doe
```

&emsp;&emsp;如图所示，您所要做的就是在字符串前加上字母s，然后在字符串内的变量名之前添加$符号，此功能称为字符串插值。Scala中的字符串插值提供了更多功能，例如还可以将变量名称括在花括号内：

```scala
scala> println(s"Name: ${firstName} ${mi} ${lastName}")
Name: John C Doe
```

&emsp;&emsp;对于一些人来说这种格式较易读，但更重要的好处是可以将表达式放在花括号内，如以下REPL示例所示：

```scala
scala> println(s"1+1 = ${1+1}")
1+1 = 2
```

&emsp;&emsp;使用字符串插值可以在字符串前面加上字母f，以便在字符串内部使用printf样式格式，而且原始插值器不对字符串内的文字（例如\\n）进行转义，另外还可以创建自己的字符串插值器。Scala字符串的另一个重要功能是可以通过将字符串包含在三个双引号中来创建多行字符串：

```scala
scala> val speech = """Four score and
| seven years ago
| our fathers ..."""
speech: String =
Four score and
seven years ago
our fathers ...
```

&emsp;&emsp;当您需要使用多行字符串时，这非常有用。这种基本方法的一个缺点是第一行之后的行是缩进的。解决此问题的简单方法是：在第一行之后的所有行前面加上符号“|”，并在字符串之后调用stripMargin方法：

```scala
scala> val speech = """Four score and
| |seven years ago
| |our fathers ...""".stripMargin
speech: String =
Four score and
seven years ago
our fathers ...
```

&emsp;&emsp;下面让我们看一下如何使用Scala处理命令行输入和输出。如前所述，可以使用以下命令println将输出写入标准输出，该函数在字符串后添加一个换行符，因此，如果您不希望这样做，只需使用print：

```scala
scala> println("Hello, world")
Hello, world
scala> print("Hello without newline")
Hello without newline
```

&emsp;&emsp;因为println是常用方法，所以同其他常用数据类型一样不需要导入它。有几种读取命令行输入的方法，但是最简单的方法是使用scala.io.StdIn包中的readLine方法。就像使用Java和其他语言一样，通过import语句将类和方法带入Scala的作用域：

```scala
scala> import scala.io.StdIn.readLine
import scala.io.StdIn.readLine
```

&emsp;&emsp;该import语句将readLine方法带入当前范围，因此可以在应用程序中使用它。Scala具有编程语言的基本控制结构，包括：条件语句（if/then/else）、for循环、异常捕获（try/catch/finally），它还具有一些独特的构造：match表达式、for表达式。我们将在以下内容中进行演示。一个基本的Scala
&emsp;&emsp;if语句如下所示：

&emsp;&emsp;if (a == b) doSomething()

&emsp;&emsp;也可以这样编写该语句：

&emsp;&emsp;if (a == b) {

&emsp;&emsp;doSomething()

&emsp;&emsp;}

&emsp;&emsp;if/ else结构如下所示：

&emsp;&emsp;if (a == b) {

&emsp;&emsp;doSomething()

&emsp;&emsp;} else {

&emsp;&emsp;doSomethingElse()

&emsp;&emsp;}

&emsp;&emsp;完整的Scala if/else-if/else表达式如下所示：

&emsp;&emsp;if (test1) {

&emsp;&emsp;doX()

&emsp;&emsp;} else if (test2) {

&emsp;&emsp;doY()

&emsp;&emsp;} else {

&emsp;&emsp;doZ()

&emsp;&emsp;}

&emsp;&emsp;Scala if构造总是返回结果，可以像前面的示例中那样忽略结果，但是更常见的方法（尤其是在函数编程中）是将结果分配给变量：

```scala
scala> val a=1
a: Int = 1
scala> val b=2
b: Int = 2
scala> val minValue = if (a < b) a else b
minValue: Int = 1
```

&emsp;&emsp;这意味着Scala不需要特殊的三元运算符。Scala
&emsp;&emsp;for循环可用于迭代集合中的元素。例如给定一个整数序列，然后遍历它们并打印出它们的值，如下所示：：

```scala
scala> val nums = Seq(1,2,3)
nums: Seq[Int] = List(1, 2, 3)
scala> for (n <- nums) println(n)
1
2
3
```

&emsp;&emsp;上面的示例使用整数序列，其数据类型为Seq\[Int\]，下面例子的数据类型为字符串列表List\[String\]，使用for循环来打印其值，就像前面的示例一样：

```scala
scala> val people = List(
| "Bill",
| "Candy",
| "Karen",
| "Leo",
| "Regina"
| )
people: List[String] = List(Bill, Candy, Karen, Leo, Regina)
scala> for (p <- people) println(p)
Bill
Candy
Karen
Leo
Regina
```

&emsp;&emsp;Seq和List是线性集合的两种类型。在Scala中，这些集合类优于Array。为了遍历元素集合并打印其内容，还可以使用foreach方法，对于Scala集合类可用的这个方法，例如用foreach来打印先前的字符串列表：

```scala
scala> people.foreach(println)
Bill
Candy
Karen
Leo
Regina
```

&emsp;&emsp;foreach 可用于大多数集合类，对于Map（类似于Java
&emsp;&emsp;的HashMap），可以使用for和foreach。下面的例子中使用Map定义电影名称和等级，分别使用for和foreach方法打印输出电影名称和等级：

```scala
scala> val ratings = Map(
| "Lady in the Water" -> 3.0,
| "Snakes on a Plane" -> 4.0,
| "You, Me and Dupree" -> 3.5
| )
ratings: scala.collection.immutable.Map[String,Double] = Map(Lady in
the Water -> 3.0, Snakes on a Plane -> 4.0, You, Me and Dupree ->
3.5)
scala> for ((name,rating) <- ratings) println(s"Movie: $name, Rating:
$rating")
Movie: Lady in the Water, Rating: 3.0
Movie: Snakes on a Plane, Rating: 4.0
Movie: You, Me and Dupree, Rating: 3.5
scala> ratings.foreach {
| case(movie, rating) => println(s"key: $movie, value: $rating")
| }
key: Lady in the Water, value: 3.0
key: Snakes on a Plane, value: 4.0
key: You, Me and Dupree, value: 3.5
```

&emsp;&emsp;在此示例中，name对应于Map中的每个键，rating是分配给每个name的值。一旦开始使用Scala，我们会发现在函数式编程语言for中，除了for循环之外，还可以使用更强大的for表达式。在Scala中，for表达式是for结构的另一种用法。例如给定以下整数列表，然后创建一个新的整数列表，其中所有值都加倍，如下所示：

```scala
scala> val nums = Seq(1,2,3)
nums: Seq[Int] = List(1, 2, 3)
scala> val doubledNums = for (n <- nums) yield n * 2
doubledNums: Seq[Int] = List(2, 4, 6)
```

&emsp;&emsp;该表达式可以理解为：对于数字nums列表中的每个数字n的值加倍，然后将所有新值分配给变量doubledNums。总而言之，for表达式的结果是将创建一个名为doubledNums的新变量，其值是通过将原始列表中nums的每个值加倍而创建的。我们可以对字符串列表使用相同的方法，例如给出以下小写字符串列表，使用for表达式创建大写的字符串列表：

```scala
scala> val names = List("adam", "david", "frank")
names: List[String] = List(adam, david, frank)
scala>
scala> val ucNames = for (name <- names) yield name.capitalize
ucNames: List[String] = List(Adam, David, Frank)
```

&emsp;&emsp;上面两个for表达式都使用yield关键字，表示使用所示算法在for表达式中迭代的现有集合产生一个新集合。如果要解决下面的问题，必须使用yield表达式，例如给定这样的字符串列表：

```scala
scala> val names = List("_adam", "_david", "_frank")
names: List[String] = List(_adam, _david, _frank)
```

&emsp;&emsp;假设我们要创建一个包含每个大写姓名的新列表。为此，首先需要删除每个名称开头的下划线字符，然后大写每个名称。要从每个名称中删除下划线，需要在每个String上调用drop(1)，完成之后在每个字符串上调用大写方法，可以通过以下方式使用for表达式解决此问题：

```scala
scala> val capNames = for (name <- names) yield {
| val nameWithoutUnderscore = name.drop(1)
| val capName = nameWithoutUnderscore.capitalize
| capName
| }
capNames: List[String] = List(Adam, David, Frank)
```

&emsp;&emsp;我们在该示例中显示了一种比较繁琐解决方案，因此可以看到在yield之后使用了多行代码。但是，对于这个特定的示例也可以使用更短的编写代码，这更像是Scala风格的：

```scala
scala> val capNames = for (name <- names) yield
name.drop(1).capitalize
capNames: List[String] = List(Adam, David, Frank)
```

&emsp;&emsp;还可以在算法周围加上花括号：

```scala
scala> val capNames = for (name <- names) yield {
name.drop(1).capitalize }
capNames: List[String] = List(Adam, David, Frank)
```

&emsp;&emsp;Scala还有一个match表达式的概念。在最简单的情况下，可以使用match类似Java
&emsp;&emsp;switch语句的表达式。使用match表达式可以编写了许多case语句，用于匹配可能的值。在示例中，我们将整数值1到12进行匹配。其他任何值都将落入最后一个符号“\_”，这是通用的默认情况。match表达式很不错，因为它们也返回值，所以您可以将字符串结果分配给新值：
```scala
val monthName = i match {
  case 1  => "January"
  case 2  => "February"
  case 3  => "March"
  case 4  => "April"
  case 5  => "May"
  case 6  => "June"
  case 7  => "July"
  case 8  => "August"
  case 9  => "September"
  case 10 => "October"
  case 11 => "November"
  case 12 => "December"
  case _  => "Invalid month"
}
```

&emsp;&emsp;另外，Scala还使将match表达式用作方法主体变得容易。作为简要介绍，下面是一个名为的方法convertBooleanToStringMessage，该方法接受一个Boolean值并返回String：

```scala
scala> def convertBooleanToStringMessage(bool: Boolean): String = {
| if (bool) "true" else "false"
| }
convertBooleanToStringMessage: (bool: Boolean)String
```

&emsp;&emsp;这些示例说明了为它提供布尔值true和false时它是如何工作的：

```scala
scala> val answer = convertBooleanToStringMessage(true)
answer: String = true
scala> val answer = convertBooleanToStringMessage(false)
answer: String = false
```

&emsp;&emsp;下面是第二个示例，与上一个示例一样工作，将Boolean值作为输入参数并返回一条String消息。最大的区别是此方法将match表达式用作方法的主体：

```scala
scala> def convertBooleanToStringMessage(bool: Boolean): String = bool
match {
| case true => "you said true"
| case false => "you said false"
| }
convertBooleanToStringMessage: (bool: Boolean)String
```

&emsp;&emsp;该方法的主体只有两个case语句，一个匹配true，另一个匹配false。因为这些是唯一可能的Boolean值，所以不需要默认case语句，可以调用该方法然后打印其结果的方式：

```scala
scala> val result = convertBooleanToStringMessage(true)
result: String = you said true
scala> println(result)
you said true
```

&emsp;&emsp;将match表达式用作方法的主体也是一种常见用法。match
&emsp;&emsp;表达式非常强大，我们将演示可以使用match执行的其他操作。match表达式可以在单个case语句中处理多种情况，为了说明这一点，假设0或空白字符串求值为false，其他任何值求为true，使用match表达式计算true和fales，这一条语句（case
&emsp;&emsp;0 | "" =\> false）让0和空字符串都可以评估为false：

```scala
scala> def isTrue(a: Any) = a match {
| case 0 | "" => false
| case _ => true
| }
isTrue: (a: Any)Boolean
```

&emsp;&emsp;因为将输入参数a定义为Any类型，这是所有Scala类的根，就像Java中的Object一样，所以此方法可与传入的任何数据类型一起使用：

```scala
scala> isTrue(0)
res0: Boolean = false
scala> isTrue("")
res1: Boolean = false
scala> isTrue(1.1F)
res2: Boolean = true
scala> isTrue(new java.io.File("/etc/passwd"))
res3: Boolean = true
```

&emsp;&emsp;match表达式的另一个优点是，可以在case语句中使用if表达式来进行强大的模式匹配。在此示例中，第二和第三种情况语句均使用if表达式来匹配数字范围：

```scala
scala> val count=1
count: Int = 1
scala> count match {
| case 1 => println("one, a lonely number")
| case x if x == 2 || x == 3 => println("two's company, three's a
crowd")
| case x if x > 3 => println("4+, that's a party")
| case _ => println("i'm guessing your number is zero or less")
| }
one, a lonely number
```

&emsp;&emsp;Scala不需要在if表达式中使用括号，但是如果使用可以提高可读性：

&emsp;&emsp;count match {

&emsp;&emsp;case 1 =\> println("one, a lonely number")

&emsp;&emsp;case x if (x == 2 || x == 3) =\> println("two's company, three's a
&emsp;&emsp;crowd")

&emsp;&emsp;case x if (x \> 3) =\> println("4+, that's a party")

&emsp;&emsp;case \_ =\> println("i'm guessing your number is zero or less")

&emsp;&emsp;}

&emsp;&emsp;为了支持面向对象编程，Scala提供了一个类构造。语法比Java和C＃之类的语言简洁得多，而且易于使用和阅读。这里有一个Scala的类，它的构造函数定义两个参数firstName和lastName：

```scala
scala> class Person(var firstName: String, var lastName: String)
defined class Person
```

&emsp;&emsp;有了这个定义，可以创建如下的新Person实例：

```scala
scala> val p = new Person("Bill", "Panner")
p: Person = Person@4e52d2f2
```

&emsp;&emsp;在类构造函数中定义参数会自动在类中创建字段，在本示例中可以像这样访问firstName和lastName字段：

```scala
scala> println(p.firstName + " " + p.lastName)
Bill Panner
```

&emsp;&emsp;在此示例中，由于两个字段都被定义为var字段，因此它们也是可变的，这意味着可以更改它们：

```scala
scala> p.firstName = "Ivan"
p.firstName: String = Ivan
scala> p.lastName = "Lee"
p.lastName: String = Lee
```

&emsp;&emsp;在上面的示例中，两个字段都被定义为var字段，这使得这些字段可变，还可以将它们定义为val字段，这使它们不可变：

```scala
scala> class Person(val firstName: String, val lastName: String)
defined class Person
scala> val p = new Person("Bill", "Panner")
p: Person = Person@496c6d94
scala> p.firstName = "Fred"
<console>:12: error: reassignment to val
p.firstName = "Fred"
^
scala> p.lastName = "Jones"
<console>:12: error: reassignment to val
p.lastName = "Jones"
^
```

&emsp;&emsp;如果使用Scala编写面向对象编程的代码，将字段创建为var字段，以便对其进行改变。当使用Scala编写函数编程的代码时，一般使用用例类而不是像这样的类。

&emsp;&emsp;在Scala中，类的构造可以包括：构造参数；类主体中调用的方法；在类主体中执行的语句和表达式。在Scala类的主体中声明的字段以类似于Java的方式处理，它们是在首次实例化该类时分配的。下面的Person类演示了可以在类体内执行的一些操作：

```scala
scala> class Person(var firstName: String, var lastName: String) {
|
| println("the constructor begins")
|
| // 'public' access by default
| var age = 0
|
| // some class fields
| private val HOME = System.getProperty("user.home")
|
| // some methods
| override def toString(): String = s"$firstName $lastName is $age years
old"
|
| def printHome(): Unit = println(s"HOME = $HOME")
| def printFullName(): Unit = println(this)
|
| printHome()
| printFullName()
| println("you've reached the end of the constructor")
|
| }
defined class Person
```

&emsp;&emsp;Scala REPL中的以下代码演示了该类的工作方式：

```scala
scala> val p = new Person("Kim", "Carnes")
the constructor begins
HOME = /Users/al
Kim Carnes is 0 years old
you've reached the end of the constructor
p: Person = Kim Carnes is 0 years old
scala> p.age
res0: Int = 0
scala> p.age = 36
p.age: Int = 36
scala> p
res1: Person = Kim Carnes is 36 years old
scala> p.printHome
HOME = /Users/al
scala> p.printFullName
Kim Carnes is 36 years old
```

&emsp;&emsp;在Scala中，方法一般是在类内部定义的（就像Java），但是也可以在REPL中创建它们。本课将显示一些方法示例，以便您可以看到语法。这是如何定义名为double的方法，该方法采用一个名为a的整数输入参数并返回该整数的2倍，方法名称和签名显示在=符号的左侧：

```scala
scala> def double(a: Int) = a * 2
double: (a: Int)Int
```

&emsp;&emsp;def是用于定义方法的关键字，方法名称为double，输入参数a的类型Int为Scala的整数类型。函数的主体显示在右侧，在此示例中，它只是将输入参数a的值加倍。将该方法粘贴到REPL之后，可以通过给它一个Int值来调用它：

```scala
scala> double(2)
res0: Int = 4
scala> double(10)
res1: Int = 20
```

&emsp;&emsp;上一个示例未显示该方法的返回类型，但是可以显示它：

```scala
scala> def double(a: Int): Int = a * 2
double: (a: Int)Int
```

&emsp;&emsp;编写这样的方法会显式声明该方法的返回类型。有些人喜欢显式声明方法返回类型，因为它使代码更容易维护。如果将该方法粘贴到REPL中，将看到它的工作方式与之前的方法相同。为了显示一些更复杂的方法，以下是一个使用两个输入参数的方法：

```scala
scala> def add(a: Int, b: Int) = a + b
add: (a: Int, b: Int)Int
```

&emsp;&emsp;当一个方法只有一行，可以使用上面的格式，但是当方法主体变长时可以将多行放在花括号内：

```scala
scala> def addThenDouble(a: Int, b: Int): Int = {
| val sum = a + b
| val doubled = sum * 2
| doubled
| }
addThenDouble: (a: Int, b: Int)Int
scala> addThenDouble(1, 1)
res0: Int = 4
```

&emsp;&emsp;Scala特质是该语言的一大特色，可以像使用Java接口一样使用它们，也可以像使用具有实际方法的抽象类一样使用它们。Scala类还可以扩展和混合多个特质。Scala还具有抽象类的概念，我们需要了解何时应该使用抽象类而不是特质。一种使用Scala特质的方法就像原始Java的接口，在其中可以为某些功能定义所需的接口，但是没有实现任何行为。举一个例子，假设想编写一些代码来模拟任何有尾巴的动物，如狗和猫。在Scala中，我们编写了一个特质来启动该建模过程，如下所示：

```scala
scala> trait TailWagger {
| def startTail(): Unit
| def stopTail(): Unit
| }
defined trait TailWagger
```

&emsp;&emsp;该代码声明了一个名为TailWagger的特质，该特质指出扩展TailWagger的任何类都应实现startTail和stopTail方法。这两种方法都没有输入参数，也没有返回值。可以编写一个扩展特质并实现如下方法的类：

```scala
scala> class Dog extends TailWagger {
| // the implemented msethods
| def startTail(): Unit = println("tail is wagging")
| def stopTail(): Unit = println("tail is stopped")
| }
defined class Dog
scala> val d = new Dog
d: Dog = Dog@5b8572df
scala> d.startTail
tail is wagging
scala> d.stopTail
tail is stopped
```

&emsp;&emsp;我们可以使用extends关键字来创建扩展单个特征的类。这演示了如何使用扩展特质类来实现其中方法。Scala允许创建具有特质的非常模块化的代码。
&emsp;&emsp;例如可以将动物的属性分解为模块化的单元：

```scala
scala> trait Speaker {
| def speak(): String
| }
defined trait Speaker
scala>
scala> trait TailWagger {
| def startTail(): Unit
| def stopTail(): Unit
| }
defined trait TailWagger
scala>
scala> trait Runner {
| def startRunning(): Unit
| def stopRunning(): Unit
| }
defined trait Runner
```

&emsp;&emsp;一旦有了这些小片段，就可以通过扩展所有它们并实现必要的方法来创建Dog类：

```scala
scala> class Dog extends Speaker with TailWagger with Runner {
|
| // Speaker
| def speak(): String = "Woof!"
|
| // TailWagger
| def startTail(): Unit = println("tail is wagging")
| def stopTail(): Unit = println("tail is stopped")
|
| // Runner
| def startRunning(): Unit = println("I'm running")
| def stopRunning(): Unit = println("Stopped running")
|
| }
defined class Dog
```

&emsp;&emsp;注意如何extends和with用于从多个特征创建类。

### 2.5.2 函数式编程

&emsp;&emsp;Scala允许我们将两种方法结合使用，以面向对象编程风格和函数式编程风格甚至混合风格编写代码。如果之前学习过Java、C
&emsp;&emsp;++或C＃之类的面向对象编程语言，这有利于我们理解相关的概念。但是，由于函数式编程风格对于许多开发人员来说仍相对较新，所以理解起来会有些难度，我们可以现从简单的概念入手。

&emsp;&emsp;函数式编程是一种编程风格，强调只使用纯函数和不可变值编写应用程序，函数式程序员非常渴望将其代码视为数学中的函数公式，并且可以将它们组合成为一系列代数方程式。使用函数式编程更像是数据科学家通过定义数据公式解决问题，驱使他们仅使用纯函数和不可变值，因为这就是我们在代数和其他形式的数学中所使用的。函数式编程是一个很大的主题，实际上通过本小结我们只是了解函数式编程，显示Scala为开发人员提供的一些用于编写功能代码的工具。首先我们使用Scala提供的函数式编程模式编写纯函数。纯函数的定义为：函数的输出仅取决于其输入变量；它不会改变任何隐藏状态；不会从外界读取数据（包括控制台、Web服务、数据库和文件等），也不会向外界写入数据。由于此定义，每次调用具有相同输入值的纯函数时，总会得到相同的结果，例如可以使用输入值2无限次调用double函数，并且始终获得结果4。按照这个定义，scala.math.\_包中的此类方法就是纯函数，例如abs、ceil、max、min，这些Scala
&emsp;&emsp;String方法也是纯函数：isEmpty、length和substring。在Scala集合类的很多方法也作为纯函数，包括drop、filter和map。

&emsp;&emsp;相反，以下功能不纯，因为它们违反了定义。与日期和时间相关的方法都不纯，例如getDayOfWeek，getHour和getMinute，因为它们的输出取决于输入参数以外的其他东西，他们的结果依赖于这些示例中某种形式的隐藏输入输出操作和隐藏输入。通常，不纯函数会执行以下一项或多项操作：

&emsp;&emsp;（1）读取隐藏的输入，访问未显式传递为输入参数的变量和数据

&emsp;&emsp;（2）写隐藏的输出

&emsp;&emsp;（3）改变它们给定的参数

&emsp;&emsp;（4）与外界进行某种读写

&emsp;&emsp;当然应用程序不可能完全与外界没有输入输出，因此人们提出以下建议：使用纯函数编写应用程序的核心，然后围绕该核心编写不纯的包装以与外界交互。用Scala编写纯函数是关于函数编程的较简单部分之一，只需使用Scala定义方法的语法编写纯函数。这是一个纯函数，将给定的输入值加倍：

```scala
scala> def double(i: Int): Int = i * 2
double: (i: Int)Int
```

&emsp;&emsp;纯函数是仅依赖于其声明的输入及其内部算法来生成其输出的函数。它不会从外部世界（函数范围之外的世界）中读取任何其他值，并且不会修改外部世界中的任何值。实际的应用程序包含纯功能和不纯功能的组合，通常的建议是使用纯函数编写应用程序的核心，然后使用不纯函数与外界进行通信。

&emsp;&emsp;尽管曾经创建的每种编程语言都可能允许我们编写纯函数，但是Scala另一个函数式编程的特点是可以将函数创建为变量，就像创建String和Int变量一样。此功能有很多好处，其中最常见的好处是可以将函数作为参数传递给其他函数，例如：

```scala
scala> val nums = (1 to 10).toList
nums: List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
scala>
scala> val doubles = nums.map(_ * 2)
doubles: List[Int] = List(2, 4, 6, 8, 10, 12, 14, 16, 18, 20)
scala> val lessThanFive = nums.filter(_ < 5)
lessThanFive: List[Int] = List(1, 2, 3, 4)
```

&emsp;&emsp;在这些示例中，匿名函数被传递到map和filter中，与将常规函数传递给相同map：

```scala
scala> def double(i: Int): Int = i * 2
double: (i: Int)Int
scala> val doubles = nums.map(double)
doubles: List[Int] = List(2, 4, 6, 8, 10, 12, 14, 16, 18, 20)
```

&emsp;&emsp;如这些示例所示，Scala显然允许您将匿名函数和常规函数传递给其他方法。这是优秀的函数式编程语言提供的强大功能。如果从技术术语角度介绍的话，将另一个函数作为输入参数的函数称为高阶函数。将函数作为变量传递的能力是函数式编程语言的一个显着特征，就像map和filter将函数作为参数传递给其他函数的能力可以帮助我们创建简洁而又易读的代码。为了更好的体验将函数作为参数传递给其他函数的过程，可以在REPL中尝试以下几个示例：

```scala
scala> List("foo", "bar").map(_.toUpperCase)
res3: List[String] = List(FOO, BAR)
scala> List("foo", "bar").map(_.capitalize)
res4: List[String] = List(Foo, Bar)
scala> List("adam", "scott").map(_.length)
res5: List[Int] = List(4, 5)
scala> List(1,2,3,4,5).map(_ * 10)
res6: List[Int] = List(10, 20, 30, 40, 50)
scala> List(1,2,3,4,5).filter(_ > 2)
res7: List[Int] = List(3, 4, 5)
scala> List(5,1,3,11,7).takeWhile(_ < 6)
res8: List[Int] = List(5, 1, 3)
```

&emsp;&emsp;这些匿名函数中的任何一个也可以写为常规函数，因此我们可以编写如下函数：

```scala
scala> def toUpper(s: String): String = s.toUpperCase
toUpper: (s: String)String
scala> List("foo", "bar").map(toUpper)
res9: List[String] = List(FOO, BAR)
scala> List("foo", "bar").map(s => toUpper(s))
res10: List[String] = List(FOO, BAR)
```

&emsp;&emsp;这些使用常规函数的示例等同于这些匿名函数示例：

```scala
scala> List("foo", "bar").map(s => s.toUpperCase)
res11: List[String] = List(FOO, BAR)
scala> List("foo", "bar").map(_.toUpperCase)
res12: List[String] = List(FOO, BAR)
```

&emsp;&emsp;函数式编程就像编写一系列代数方程式一样，并且由于在代数中不使用空值，因此在函数式编程中不使用空值。Scala的解决方案是使用构造，例如Option/Some/None类。虽然第一个Option/Some/None示例不处理空值，但这是演示Option/Some/None类的好方法，因此我们从它开始。

&emsp;&emsp;想象一下，我们想编写一种方法来简化将字符串转换为整数值的过程，并且想要一种优雅的方法来处理当获取的字符串类似“foo”而不能转换为数字时可能引发的异常。对这种函数的首次猜测可能是这样的：

```scala
scala> def toInt(s: String): Int = {
| try {
| Integer.parseInt(s.trim)
| } catch {
| case e: Exception => 0
| }
| }
toInt: (s: String)Int
```

&emsp;&emsp;此函数的思路是：如果字符串转换为整数，则返回整数，但如果转换失败，则返回0。出于某些目的这可能还可以，但实际上并不准确。例如该方法可能会接收到“0”，但是也可能是“foo”或者也可能收到“bar”等无数其他字符串。这就产生了一个实际的问题：怎么知道该方法何时真正收到“0”，或何时收到其他东西？但是，使用这种方法无法知道。Scala解决这个问题的方法是使用三个类：Option、Some和None。Some与None类是Option的子类，因此解决方案是这样的：

&emsp;&emsp;（1）声明toInt返回一个Option类型

&emsp;&emsp;（2）如果toInt收到一个可以转换为Int的字符串，则将Int包裹在Some中

&emsp;&emsp;（3）如果toInt收到无法转换的字符串，则返回None

&emsp;&emsp;解决方案的实现如下所示：

```scala
scala> def toInt(s: String): Option[Int] = {
| try {
| Some(Integer.parseInt(s.trim))
| } catch {
| case e: Exception => None
| }
| }
toInt: (s: String)Option[Int]
```

&emsp;&emsp;这段代码可以理解为：当给定的字符串转换为整数时，返回Some包装器中的整数，例如Some(1)，如果字符串不能转换为整数，则返回None值。以下是两个REPL示例，它们演示了toInt的实际作用：

```scala
scala> val a = toInt("1")
a: Option[Int] = Some(1)
scala> val a = toInt("foo")
a: Option[Int] = None
```

&emsp;&emsp;如图所示，字符串“1”转换为Some(1)，而字符串“foo”转换为None。这是Option/Some/None方法的本质，用于处理异常（如本例所示），并且相同的技术也可用于处理空值，我们会发现整个Scala库类以及第三方Scala库都使用了这种方法。

&emsp;&emsp;现在，假设我们是该toInt方法的使用者，该方法返回Option\[Int\]
&emsp;&emsp;的子类，所以问题就变成了，如何使用这些返回类型？根据需求主要有两个答案：（1）使用match表达式；（2）使用表达式。还有其他方法，但是这是两个主要方法，特别是从函数式编程的角度来看。一种可能是使用match表达式，如下所示：

&emsp;&emsp;toInt(x) match {

&emsp;&emsp;case Some(i) =\> println(i)

&emsp;&emsp;case None =\> println("That didn't work.")

&emsp;&emsp;}

&emsp;&emsp;在此示例中，如果x可以转换为Int，case则执行第一条语句；如果x不能转换为Int，case则执行第二条语句。另一个常见的解决方案是使用for/yield组合。为了证明这一点，假设将三个字符串转换为整数值，然后将它们加在一起。for/yield解决方案如下所示：

```scala
scala> val stringA = "1"
stringA: String = 1
scala> val stringB = "2"
stringB: String = 2
scala> val stringC = "3"
stringC: String = 3
scala> val y = for {
| a <- toInt(stringA)
| b <- toInt(stringB)
| c <- toInt(stringC)
| } yield a + b + c
y: Option[Int] = Some(6)
```

&emsp;&emsp;该表达式结束运行时，y将是以下两件事之一：

&emsp;&emsp;（1）如果所有三个字符串都转换为整数，y则将为Some\[Int\]，即包装在Some内的整数

&emsp;&emsp;（2）如果三个字符串中的任何一个都不能转换为内部字符串，y将为None

&emsp;&emsp;可以在Scala
&emsp;&emsp;REPL中自己对此进行测试，输入三个字符串变量，y的值为Some(6)。另一种情况是将所有这些字符串更改为不会转换为整数的字符串，我们会看到y的值为None。考虑Option类的一种好方法是：将其看做一个容器，更具体地说是一个内部包含0或1项的容器，Some是其中只有一件物品的容器，None也是一个容器，但是里面什么也没有。

&emsp;&emsp;因为可以将Some和None视为容器，所以可以将它们进一步视为类似于集合类。
&emsp;&emsp;因此，它们具有应用于集合类的所有方法，包括map、filter、foreach等，例如：

```scala
scala> toInt("1").foreach(println)
1
scala> toInt("x").foreach(println)
```

&emsp;&emsp;第一个示例显示数字1，而第二个示例不显示任何内容。这是因为toInt("1")计算为
&emsp;&emsp;Some(1)，Some类上的foreach方法知道如何从Some容器内部提取其中的值，因此将该值传递给println。同样，第二个示例不打印任何内容，因为toInt("x")计算为
&emsp;&emsp;None，None类上的foreach方法知道None不包含任何内容，因此不执行任何操作。

### 2.5.3 集合类

&emsp;&emsp;Scala集合类是一个易于理解且经常使用的编程抽象，可以分为可变集合和不可变集合。可变集合可以在必要时进行更改、更新或扩展，但是不可变集合不能更改。大多数集合类分别位于包scala.collection、scala.collection.immutable和scala.collection.mutable中。我们使用的主要Scala集合类是：

| 类           | 描述           |
| ----------- | ------------ |
| ArrayBuffer | 索引的可变序列      |
| List        | 线性（链表），不可变序列 |
| Vector      | 索引不变的序列      |
| Map         | 基本Map（键/值对）类 |
| Set         | 基本Set类       |

&emsp;&emsp;ArrayBuffer是一个可变序列，因此可以使用其方法来修改其内容，并且这些方法类似于Java序列上的方法。要使用ArrayBuffer必须先将其导入：

```scala
scala> import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ArrayBuffer
```

&emsp;&emsp;将其导入本地范围后，将创建一个空的ArrayBuffer，可以通过多种方式向其中添加元素，如下所示：

```scala
scala> val ints = ArrayBuffer[Int]()
ints: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer()
scala> ints += 1
res17: ints.type = ArrayBuffer(1)
scala> ints += 2
res18: ints.type = ArrayBuffer(1, 2)
```

&emsp;&emsp;这只是创建ArrayBuffer并向其中添加元素的一种方法，还可以使用以下初始元素创建ArrayBuffer，通过以下几种方法向此ArrayBuffer添加更多元素：

```scala
scala> val nums = ArrayBuffer(1, 2, 3)
nums: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(1, 2, 3)
scala> nums += 4
res19: nums.type = ArrayBuffer(1, 2, 3, 4)
scala> nums += 5 += 6
res20: nums.type = ArrayBuffer(1, 2, 3, 4, 5, 6)
scala> nums ++= List(7, 8, 9)
res21: nums.type = ArrayBuffer(1, 2, 3, 4, 5, 6, 7, 8, 9)
```

&emsp;&emsp;还可以使用“-=”和“-=”方法从ArrayBuffer中删除元素：

```scala
scala> nums -= 9
val res3: ArrayBuffer[Int] = ArrayBuffer(1, 2, 3, 4, 5, 6, 7, 8)
scala> nums -= 7 -= 8
val res4: ArrayBuffer[Int] = ArrayBuffer(1, 2, 3, 4, 5, 6)
scala> nums --= Array(5, 6)
val res5: ArrayBuffer[Int] = ArrayBuffer(1, 2, 3, 4)
```

&emsp;&emsp;简要概述一下，可以将以下几种方法用于ArrayBuffer：

```scala
scala> val a = ArrayBuffer(1, 2, 3) // ArrayBuffer(1, 2, 3)
a: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(1, 2, 3)
scala> a.append(4) // ArrayBuffer(1, 2, 3, 4)
scala> a.append(5, 6) // ArrayBuffer(1, 2, 3, 4, 5, 6)
scala> a.appendAll(Seq(7,8)) // ArrayBuffer(1, 2, 3, 4, 5, 6, 7, 8)
scala> a.clear // ArrayBuffer()
scala>
scala> val a = ArrayBuffer(9, 10) // ArrayBuffer(9, 10)
a: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(9, 10)
scala> a.insert(0, 8) // ArrayBuffer(8, 9, 10)
scala> a.insertAll(0, Vector(4, 5, 6, 7)) // ArrayBuffer(4, 5, 6, 7, 8,
9, 10)
scala> a.prepend(3) // ArrayBuffer(3, 4, 5, 6, 7, 8, 9, 10)
scala> a.prepend(1, 2) // ArrayBuffer(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
scala> a.prependAll(Array(0)) // ArrayBuffer(0, 1, 2, 3, 4, 5, 6, 7, 8,
9, 10)
scala>
scala> val a = ArrayBuffer.range('a', 'h') // ArrayBuffer(a, b, c, d,
e, f, g)
a: scala.collection.mutable.ArrayBuffer[Char] = ArrayBuffer(a, b, c,
d, e, f, g)
scala> a.remove(0) // ArrayBuffer(b, c, d, e, f, g)
res44: Char = a
scala> a.remove(2, 3) // ArrayBuffer(b, c, g)
scala>
scala> val a = ArrayBuffer.range('a', 'h') // ArrayBuffer(a, b, c, d,
e, f, g)
a: scala.collection.mutable.ArrayBuffer[Char] = ArrayBuffer(a, b, c,
d, e, f, g)
scala> a.trimStart(2) // ArrayBuffer(c, d, e, f, g)
scala> a.trimEnd(2) // ArrayBuffer(c, d, e)
```

&emsp;&emsp;List类是线性的，不可变的序列。这意味着它是一个无法修改的链表，每当要添加或删除List元素时，都可以从一个现存的List中创建一个新元素List。这是创建初始列表的方法：

```scala
scala> val ints = List(1, 2, 3)
ints: List[Int] = List(1, 2, 3)
scala> val names = List("Joel", "Chris", "Ed")
names: List[String] = List(Joel, Chris, Ed)
```

&emsp;&emsp;由于列表是不可变的，因此无法向其中添加新元素。相反，可以通过在现有列表之前或之后添加元素来创建新列表，例如给定此列表：

```scala
scala> val a = List(1,2,3)
a: List[Int] = List(1, 2, 3)
scala> val b = 0 +: a
b: List[Int] = List(0, 1, 2, 3)
scala> val b = List(-1, 0) ++: a
b: List[Int] = List(-1, 0, 1, 2, 3)
```

&emsp;&emsp;也可以将元素追加到List，但是由于List是单链接列表，因此实际上只应在元素之前添加元素；向其添加元素是一个相对较慢的操作，尤其是在处理大序列时。如果要在不可变序列的前面和后面添加元素，需要使用Vector。由于列表是链接列表类，因此不应尝试通过大列表的索引值来访问它们。例如，如果具有一个包含一百万个元素的列表，则访问myList(999999)之类的元素将花费很长时间，如果要访问这样的元素，需要使用Vector或ArrayBuffer。下面的例子展示如何遍历列表的语法，给定这样的List：

```scala
scala> val names = List("Joel", "Chris", "Ed")
names: List[String] = List(Joel, Chris, Ed)
scala> for (name <- names) println(name)
Joel
Chris
Ed
```

&emsp;&emsp;关于这种方法的最大好处是，它适用于所有的序列类，包括ArrayBuffer、List、Seq和Vector等。确实还可以通过以下方式创建完全相同的列表：

```scala
scala> val list = 1 :: 2 :: 3 :: Nil
list: List[Int] = List(1, 2, 3)
```

&emsp;&emsp;这是有效的，因为一个List是以Nil元素结尾的单链列表。

&emsp;&emsp;Vector类是一个索引的，不变的序列，可以通过Vector元素的索引值非常快速地访问它们，例如访问listOfPeople(999999)。通常，除了对Vector进行索引和不对List进行索引的区别外，这两个类的工作方式相同。我们可以通过以下几种方法创建Vector：

```scala
scala> val nums = Vector(1, 2, 3, 4, 5)
nums: scala.collection.immutable.Vector[Int] = Vector(1, 2, 3, 4, 5)
scala>
scala> val strings = Vector("one", "two")
strings: scala.collection.immutable.Vector[String] = Vector(one, two)
```

&emsp;&emsp;由于Vector是不可变的，因此无法向其中添加新元素，可以通过将元素追加或添加到现有Vector上来创建新序列。例如给定此向量：

```scala
scala> val a = Vector(1,2,3)
a: Vector[Int] = List(1, 2, 3)
scala> val b = a :+ 4
b: Vector[Int] = List(1, 2, 3, 4)
scala> val b = a ++ Vector(4, 5)
b: Vector[Int] = List(1, 2, 3, 4, 5)
```

&emsp;&emsp;您也可以在前面加上这样的内容：

```scala
scala> val b = 0 +: a
b: Vector[Int] = List(0, 1, 2, 3)
scala> val b = Vector(-1, 0) ++: a
b: Vector[Int] = List(-1, 0, 1, 2, 3)
```

&emsp;&emsp;因为Vector不是链表（如List），所以可以在它的前面和后面添加元素，并且两种方法的速度应该相似，循环遍历Vector元素，就像ArrayBuffer或List：

```scala
scala> val names = Vector("Joel", "Chris", "Ed")
val names: Vector[String] = Vector(Joel, Chris, Ed)
scala> for (name <- names) println(name)
Joel
Chris
Ed
```

&emsp;&emsp;Map类文档将Map描述为由键值对组成的可迭代序列。一个简单的Map看起来像这样：

```scala
scala> val states = Map(
| "AK" -> "Alaska",
| "IL" -> "Illinois",
| "KY" -> "Kentucky"
| )
states: scala.collection.immutable.Map[String,String] = Map(AK ->
Alaska, IL -> Illinois, KY -> Kentucky)
```

&emsp;&emsp;Scala具有可变和不变的Map类。在本课程中，我们将展示如何使用可变的类。要使用可变Map类，请首先导入它：

```scala
scala> import scala.collection.mutable.Map
import scala.collection.mutable.Map
```

&emsp;&emsp;然后可以创建一个像这样的Map：

```scala
scala> val states = collection.mutable.Map("AK" -> "Alaska")
states: scala.collection.mutable.Map[String,String] = Map(AK ->
Alaska)
```

&emsp;&emsp;现在，可以使用+ =向Map添加一个元素，如下所示：

```scala
scala> states += ("AL" -> "Alabama")
res49: states.type = Map(AL -> Alabama, AK -> Alaska)
```

&emsp;&emsp;还可以使用+=添加多个元素：

```scala
scala> states += ("AR" -> "Arkansas", "AZ" -> "Arizona")
res50: states.type = Map(AZ -> Arizona, AL -> Alabama, AR ->
Arkansas, AK -> Alaska)
```

&emsp;&emsp;可以使用“++=”从其他Map添加元素：

```scala
scala> states ++= Map("CA" -> "California", "CO" -> "Colorado")
res51: states.type = Map(CO -> Colorado, AZ -> Arizona, AL ->
Alabama, CA -> California, AR -> Arkansas, AK -> Alaska)
```

&emsp;&emsp;使用“-=”和“-=”并指定键值从Map中删除元素，如以下示例所示：

```scala
scala> states -= "AR"
res52: states.type = Map(CO -> Colorado, AZ -> Arizona, AL ->
Alabama, CA -> California, AK -> Alaska)
scala> states -= ("AL", "AZ")
res53: states.type = Map(CO -> Colorado, CA -> California, AK ->
Alaska)
scala> states --= List("AL", "AZ")
res54: states.type = Map(CO -> Colorado, CA -> California, AK ->
Alaska)
```

&emsp;&emsp;可以通过将Map元素的键重新分配为新值来更新它们：

```scala
scala> states("AK") = "Alaska, A Really Big State"
scala> states
res6: scala.collection.mutable.Map[String,String] = Map(CO ->
Colorado, CA -> California, AK -> Alaska, A Really Big State)
```

&emsp;&emsp;有几种不同的方法可以迭代Map中的元素，给定一个样本Map：

```scala
scala> val ratings = Map(
| "Lady in the Water"-> 3.0,
| "Snakes on a Plane"-> 4.0,
| "You, Me and Dupree"-> 3.5
| )
ratings: scala.collection.mutable.Map[String,Double] = Map(Snakes on a
Plane -> 4.0, Lady in the Water -> 3.0, You, Me and Dupree -> 3.5)
```

&emsp;&emsp;循环所有Map元素的一种好方法是使用以下for循环语法：

```scala
scala> for ((k,v) <- ratings) println(s"key: $k, value: $v")
key: Snakes on a Plane, value: 4.0
key: Lady in the Water, value: 3.0
key: You, Me and Dupree, value: 3.5
```

&emsp;&emsp;将match表达式与foreach方法一起使用也很容易理解：

```scala
scala> ratings.foreach {
| case(movie, rating) => println(s"key: $movie, value: $rating")
| }
key: Snakes on a Plane, value: 4.0
key: Lady in the Water, value: 3.0
key: You, Me and Dupree, value: 3.5
```

&emsp;&emsp;Scala Set类是一个可迭代的集合，没有重复的元素。Scala具有可变和不变的Set类。
&emsp;&emsp;在本课程中，我们将展示如何使用可变的类。要使用可变的Set，首先导入它：

```scala
scala> val set = scala.collection.mutable.Set[Int]()
set: scala.collection.mutable.Set[Int] = Set()
```

&emsp;&emsp;可以使用“+=”、“++=”将元素添加到可变Set中，还有add()方法。这里有一些例子：

```scala
scala> set += 1
val res0: scala.collection.mutable.Set[Int] = Set(1)
scala> set += 2 += 3
val res1: scala.collection.mutable.Set[Int] = Set(1, 2, 3)
scala> set ++= Vector(4, 5)
val res2: scala.collection.mutable.Set[Int] = Set(1, 5, 2, 3, 4)
```

&emsp;&emsp;如果您尝试将值添加到其中已存在的集合中，则该尝试将被忽略：

```scala
scala> set += 2
val res3: scala.collection.mutable.Set[Int] = Set(1, 5, 2, 3, 4)
```

&emsp;&emsp;Set还具有add方法，如果将元素添加到集合中，则返回true；如果未添加元素，则返回false：

```scala
scala> set.add(6)
res4: Boolean = true
scala> set.add(5)
res5: Boolean = false
```

&emsp;&emsp;可以使用“-=”和“-=”方法从集合中删除元素，如以下示例所示：

```scala
scala> val set = scala.collection.mutable.Set(1, 2, 3, 4, 5)
set: scala.collection.mutable.Set[Int] = Set(2, 1, 4, 3, 5)
// one element
scala> set -= 1
res0: scala.collection.mutable.Set[Int] = Set(2, 4, 3, 5)
// two or more elements (-= has a varargs field)
scala> set -= (2, 3)
res1: scala.collection.mutable.Set[Int] = Set(4, 5)
// multiple elements defined in another sequence
scala> set --= Array(4,5)
res2: scala.collection.mutable.Set[Int] = Set()
```

&emsp;&emsp;如上例所示，还有更多使用集合的方法，包括clear和remove：

```scala
scala> val set = scala.collection.mutable.Set(1, 2, 3, 4, 5)
set: scala.collection.mutable.Set[Int] = Set(2, 1, 4, 3, 5)
// clear
scala> set.clear()
scala> set
res0: scala.collection.mutable.Set[Int] = Set()
// remove
scala> val set = scala.collection.mutable.Set(1, 2, 3, 4, 5)
set: scala.collection.mutable.Set[Int] = Set(2, 1, 4, 3, 5)
scala> set.remove(2)
res1: Boolean = true
scala> set
res2: scala.collection.mutable.Set[Int] = Set(1, 4, 3, 5)
scala> set.remove(40)
res3: Boolean = false
```




