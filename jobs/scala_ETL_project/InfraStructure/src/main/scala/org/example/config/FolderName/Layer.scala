package org.example.config.FolderName

sealed trait Layer {
  def name: String
}

case object Raw extends Layer { val name = "Raw" }
case object Stage extends Layer { val name = "Stage" }
