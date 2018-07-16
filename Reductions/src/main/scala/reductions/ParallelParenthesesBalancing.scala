package reductions

import common.parallel
import org.scalameter._

import scala.math.min

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer (new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    */
  def balance(chars: Array[Char]): Boolean = {
    def parentheses(chars: Array[Char]): List[Char] = chars.toList filter (char => char == '(' || char == ')')

    def simpleBalance(opening: Int, chars: List[Char]): Boolean = {
      if (chars.isEmpty) opening == 0
      else if (opening < 0) false
      else {
        chars.head match {
          case '(' => simpleBalance(opening + 1, chars.tail)
          case ')' => simpleBalance(opening - 1, chars.tail)
        }
      }
    }

    simpleBalance(0, parentheses(chars))
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
    */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int): (Int, Int) = {

      /**
        *
        * @param chars char array
        * @param acc   (unmatched left parentheses, unmatched right parentheses)
        * @return (unmatched left parentheses, unmatched right parentheses) in the array
        */
      def traverseHelper(chars: Array[Char], acc: (Int, Int)): (Int, Int) = {
        chars match {
          case y if y.isEmpty => acc
          case _ => chars.head match {
            case '(' => traverseHelper(chars.tail, (acc._1 + 1, acc._2))
            case ')' => acc._1 match {
              case x if x > 0 => traverseHelper(chars.tail, (acc._1 - 1, acc._2))
              case _ => traverseHelper(chars.tail, (acc._1, acc._2 + 1))
            }
            case _ => traverseHelper(chars.tail, acc)
          }
        }
      }

      traverseHelper(chars.slice(idx, until), (arg1, arg2))
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      until - from match {
        case x if x <= threshold => traverse(from, until, 0, 0)
        case _ => val mid = (from + until) / 2
          val (r1, r2) = parallel(reduce(from, mid), reduce(mid, until))
          val matchedParentheses = min(r1._1, r2._2)
          (r1._1 + r2._1 - matchedParentheses, r1._2 + r2._2 - matchedParentheses)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
