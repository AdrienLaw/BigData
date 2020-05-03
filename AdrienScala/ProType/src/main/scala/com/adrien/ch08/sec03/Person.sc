package com.adrien.ch08.sec03

// 定义在父包
package object people {
  val defaultName = "John Q. Public"
}

object Sec03Main extends App {
  val john = new com.adrien.ch08.sec03.Person
  println(john.description)
}

class Person {
    var name = defaultName // A constant from the package
    def description = "A person with name " + name
  }
