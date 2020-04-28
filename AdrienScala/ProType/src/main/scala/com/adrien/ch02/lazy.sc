def init(): String = {
  println("call init()")
  return ""
}


def noLazy() {
  val property = init();//没有使用lazy修饰
  println("after init()")
  println(property)
}

def lazyed() {
  lazy val property = init();//没有使用lazy修饰
  println("after init()")
  println(property)
}

//先初始化，再被赋值
noLazy()

//先不初始化，直接采用赋的值
lazyed()
