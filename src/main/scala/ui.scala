object ui {
  def greeting(): Unit = {
    // this will either be the main ui or the CLI interface
    // with the main ui on the web
    println("COVID ANALYSIS")
    println("")
    println("Please visit <hostname:port> in your web browser")
  }
}
