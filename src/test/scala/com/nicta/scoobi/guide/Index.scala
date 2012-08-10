package com.nicta.scoobi
package guide

/**
 * This class generates the index.html page used for landing users on http://NICTA.github.com/scoobi
 */
class Index extends ScoobiPage { def is =
  "Scoobi".title ^
  "Welcome!"     ^
  ReadMe.is      ^
  include((new UserGuide).hide)
}
