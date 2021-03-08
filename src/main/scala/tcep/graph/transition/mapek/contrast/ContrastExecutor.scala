package tcep.graph.transition.mapek.contrast

import tcep.graph.transition.ExecutorComponent
/**
  * Created by Niels on 18.02.2018.
  *
  * responsible for executing transitions between system configurations (only placement algorithms for now)
  * @param mapek reference to the running MAPEK instance
  */
class ContrastExecutor(mapek: ContrastMAPEK) extends ExecutorComponent(mapek)