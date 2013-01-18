package gui

import javax.swing.ImageIcon

object Images {
  lazy val imagesDir = "/images/"
  def getImage(url: String) = getIcon(url).getImage
  def getIcon(url: String) = new ImageIcon(getClass.getResource(imagesDir+url))
}
