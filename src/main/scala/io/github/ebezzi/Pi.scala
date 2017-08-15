import scala.util.Random
import java.lang.Math

object Main extends App{

  //  type Vec = CStruct2[Float,Float]

  case class Complex(real : Double, imaginary : Double) {
    def norm : Double = Math.sqrt(real*real + imaginary*imaginary)
  }

  def extractNumber() : Complex = Complex(Random.nextFloat(),Random.nextFloat())

  //  def norm(vec : Ptr[Vec] ) : Double = {
  //    Math.sqrt(!vec._1 * !vec._1 + !vec._2 * !vec._2)
  //  }

  //  val newNumber = stackalloc[Vec]
  @annotation.tailrec
  def fillUntil(remaining : Int, estimate : Int , done : Int) : (Int,Int) = {
    if (remaining > 0 ){
      fillUntil(remaining - 1 , estimate + (if ( extractNumber().norm < 1.0d)  1 else 0) , done + 1 )
    } else {
      (estimate ,done )
    }
  }

  val before = System.nanoTime()
  val (estimate,done) = fillUntil(args.head.toInt,0,0)
  val result = estimate.toFloat / done* 4
  val after = System.nanoTime()
  println( s"result is $result")
  println( s"time passed : ${(after - before) / 1e6}")


}