package org.nibali.klister

object RunConfig {
  def parse(args:Array[String]) = {
    val defaults = new RunConfig();
    new scopt.OptionParser[RunConfig]("klister") {
	  head("klister", "0.0.0")
	  opt[Int]("nodes") optional() valueName(s"[=${defaults.nodes}]") action {(x, c) =>
	    c.copy(nodes = x)
	  } text("number of computational nodes")
	  opt[Int]("records") optional() valueName(s"[=${defaults.records}]") action {(x, c) =>
	    c.copy(records = x)
	  } text("approximate number of records to sample from the data set")
	  opt[String]("joinType") optional() valueName(s"[=${defaults.joinType}]") action {(x, c) =>
	    c.copy(joinType = x)
	  } text("approximate number of records to sample from the data set")
	  opt[String]("awsAccessKeyId") optional() valueName(s"[=${defaults.awsAccessKeyId}]") action {(x, c) =>
	    c.copy(awsAccessKeyId = x)
	  } text("AWS access key ID for S3 access")
	  opt[String]("awsSecretAccessKey") optional() valueName(s"[=${defaults.awsSecretAccessKey}]") action {(x, c) =>
	    c.copy(awsSecretAccessKey = x)
	  } text("AWS secret access key for S3 access")
	  arg[String]("inputPath") unbounded() required() action { (x, c) =>
	  	c.copy(inputPath = x) } text("path to the input data set")
	}.parse(args, new RunConfig())
  }
}

case class RunConfig(
  nodes: Int = 4,
  records: Int = 4000,
  inputPath: String = "",
  joinType: String = "similarity-banding",
  awsAccessKeyId: String = "",
  awsSecretAccessKey: String = ""
)
