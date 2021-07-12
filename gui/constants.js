// constants used by tcep-gui server application
// set server name to simulator for docker stack deployment -> requests to TCEPSocket must go to simulator container, not gui container
const TCEP_SERVER = "20.0.0.15"
const TCEP_PORT = "25001"
const GUI_PORT = 3000
const INTERACTIVE_SIMULATION_ENABLED = false

module.exports = {
    TCEP_SERVER,
    TCEP_PORT,
    GUI_PORT,
    INTERACTIVE_SIMULATION_ENABLED
}
