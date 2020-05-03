package com.adrien.ch08.sec04

package object people {
  val defaultName = "John Q. Public"
}

object Sec04Main extends App {
  val john = new com.adrien.ch08.sec04.Person
  println(john.description)
}

private[sec04] class Person {
    var name = defaultName // A constant from the package
    private[sec04] def description = "A person with name " + name
    private[people] def description2 = "A person with name " + name
  }

