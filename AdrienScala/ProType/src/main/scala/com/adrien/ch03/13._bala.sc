List(1,7,2,9).reduceLeft(_-_)
/*((1-7)-2)-9 = -17*/


List(1,7,2,9).reduceRight(_-_)
/*1-(7-(2-9)) = -13*/


List(1,7,2,9).foldLeft(0)(_-_)
/*0-1-7-2-9 = -19*/

val freq = scala.collection.mutable.Map[Char,Int]()
for (c <- "Mississippi") freq(c) = freq.getOrElse(c,0) + 1
freq

//HashMap(p -> 2, s -> 4, i -> 4, M -> 1)

(1 to 10).scanLeft(0)(_+_)

val princes = List(5.0,20.0,9.95)
val quantities = List(10,2,1)
princes zip quantities

List(5.0,2.0,9.95) zip List(10,2)

"Scala".zipWithIndex

