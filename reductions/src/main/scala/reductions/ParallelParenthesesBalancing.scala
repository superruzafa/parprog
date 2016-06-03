package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

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
    val length = chars.length
    @tailrec
    def balanceRec(i: Int, count: Int): Boolean = {
      if (i == length)
        count == 0
      else if (chars(i) == ')' && count == 0)
        false
      else {
        val newCount = chars(i) match {
          case '(' => 1
          case ')' => -1
          case _ => 0
        }
        balanceRec(i + 1, count + newCount)
      }
    }
    balanceRec(0, 0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    require(threshold > 0)

    /**
      * Computes the parentheses balance and sign of a isolated string chunk
      *
      * @param idx Current character being iterated
      * @param until Final position
      * @param balance Balance of the current iteration
      * @param sign Sign of the string chunk
      * @return Parentheses balance count and sign of the string chunk
      */
    @tailrec
    def traverse(idx: Int, until: Int, balance: Int, sign: Int): (Int, Int) = {
      if (idx >= until)
        (balance, sign)
      else {
        val (newBalance, newSign) = chars(idx) match {
          case '(' => (balance + 1, if (sign == 0) +1 else sign) // once the first parentheses has been found
          case ')' => (balance - 1, if (sign == 0) -1 else sign) // the sign is set and it's never changed for this chunk
          case _ => (balance, sign)
        }
        traverse(idx + 1, until, newBalance, newSign)
      }
    }

    /**
      * Reduces the sign of two subtrees
      *
      * A tree has negative sign if the first parentheses that was found in its string chunk was a closing one
      * A tree has no sign (0) if no parentheses was found (neither opening or closing) in its string chunk
      * A tree has positive sign if the first parentheses that was found in its string chunk was an opening one
      *
      * Examples:
      *
      * ")((("        -> -1, because first parentheses found was ')'
      * "hello)       -> -1, because first parentheses found was ')'
      * "hello"       ->  0, because no parentheses was found
      * "(hello       -> +1, because first parentheses found was '('
      * "hello(world) -> +1, because first parentheses found was '('
      *
      * Reduction rules:
      *
      *  left   |  right  | node sign |         example
      * subtree | subtree |  result   |
      * --------+---------+-----------+--------------------------
      *    -    |    -    |     -     | foo) ⊗ )bar --> foo)bar)
      *    -    |    0    |     -     | foo) ⊗ bar  --> foo)bar
      *    -    |    +    |     -     | foo) ⊗ bar( --> foo)bar(
      *    0    |    -    |     -     | foo  ⊗ bar) --> foobar)
      *    0    |    0    |     0     | foo  ⊗ bar  --> foobar
      *    0    |    +    |     +     | foo  ⊗ (bar --> foo(bar
      *    +    |    -    |     +     | (foo ⊗ )bar --> (foo)bar
      *    +    |    0    |     +     | (foo ⊗ bar  --> (foobar
      *    +    |    +    |     +     | (foo ⊗ (bar --> (foo(bar
      *
      * @param signLeft Sign of the left subtree
      * @param signRight Sign of the right subtree
      * @return Reduced sign
      */
    def reduceSign(signLeft: Int, signRight: Int): Int =
      if (signLeft == 0) signRight else signLeft

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until <= from || until - from <= threshold) {
        traverse(from, until, 0, 0)
      } else {
        val mid = from + (until - from) / 2
        val ((balanceLeft, signLeft), (balanceRight, signRight)) = parallel(
          reduce(from, mid),
          reduce(mid, until)
        )
        (balanceLeft + balanceRight, reduceSign(signLeft, signRight))
      }
    }

    val (balance, sign) = reduce(0, chars.length)
    balance == 0 && sign >= 0
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
