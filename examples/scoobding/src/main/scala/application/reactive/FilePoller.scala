/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactive

import java.io.File

class FilePoller(path: Signal[String], delay: Long = 500) extends Trigger {

  private var previousLastModified = new File(path.now).lastModified()

  val timer = new Timer(0, delay, {t =>  false}) foreach { tick =>
    def newLastModified = new File(path.now).lastModified()
    if (newLastModified > previousLastModified || newLastModified == 0) {
      previousLastModified = newLastModified
      source.fire(())
    }
  }

}

