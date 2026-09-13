import { h } from 'vue'
import DefaultTheme from 'vitepress/theme'
import AlphaNotice from './AlphaNotice.vue'
import VersionSelector from './VersionSelector.vue'
import './custom.css'

export default {
  extends: DefaultTheme,
  Layout() {
    return h(DefaultTheme.Layout, null, {
      'home-hero-before': () => h('div', { class: 'home-alpha-notice' }, [h(AlphaNotice)]),
      'doc-before': () => h(AlphaNotice)
    })
  },
  enhanceApp({ app }) {
    app.component('VersionSelector', VersionSelector)
  }
}
