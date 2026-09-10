import DefaultTheme from 'vitepress/theme'
import VersionSelector from './VersionSelector.vue'
import './custom.css'

export default {
  extends: DefaultTheme,
  enhanceApp({ app }) {
    app.component('VersionSelector', VersionSelector)
  }
}
