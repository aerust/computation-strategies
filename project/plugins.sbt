resolvers ++= Seq(
  Resolver.url("socrata releases", url("https://repository-socrata-oss.forge.cloudbees.com/artifactory/ivy-libs-release"))(Resolver.ivyStylePatterns)
)

addSbtPlugin("com.socrata" % "socrata-sbt-plugins" %"1.6.8")
