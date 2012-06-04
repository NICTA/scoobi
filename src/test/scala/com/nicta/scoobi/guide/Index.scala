package com.nicta.scoobi.guide

/**
 * This class generates the index.html page used for landing users on http://NICTA.github.com/scoobi
 */
class Index extends ScoobiPage { def is =
  "Welcome!".title.baseDirIs(".").urlIs("README.md") ^
  ReadMe.is ^
  include((new UserGuide).hide)
}
