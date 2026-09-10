<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

const props = defineProps<{
  current: string
  screenMenu?: boolean
}>()

const releases = ref<string[]>(props.current.startsWith('v') ? [props.current] : [])
const developmentLink = computed(() => releases.value.length === 0 ? '/' : '/development/')

function navigate(event: MouseEvent) {
  const link = event.currentTarget as HTMLAnchorElement
  window.location.assign(link.href)
}

onMounted(async () => {
  try {
    const response = await fetch('/versions.json')
    if (!response.ok) return
    const manifest: unknown = await response.json()
    if (typeof manifest === 'object' && manifest !== null &&
        'versions' in manifest && Array.isArray(manifest.versions)) {
      releases.value = manifest.versions.filter((version: unknown): version is string =>
        typeof version === 'string' && /^v\d+\.\d+\.\d+$/.test(version)
      )
    }
  } catch {
    // Keep the server-rendered current-version fallback when offline.
  }
})
</script>

<template>
  <details class="VersionSelector" :class="{ screen: screenMenu }">
    <summary>{{ current }}</summary>
    <div class="menu">
      <a :href="developmentLink" @click.prevent.stop="navigate">Development</a>
      <a
        v-for="version in releases"
        :key="version"
        :href="`/${version}/`"
        @click.prevent.stop="navigate"
      >
        {{ version }}
      </a>
    </div>
  </details>
</template>

<style scoped>
.VersionSelector {
  position: relative;
  height: var(--vp-nav-height);
  color: var(--vp-c-text-1);
  font-size: 14px;
  font-weight: 500;
}

summary {
  display: flex;
  align-items: center;
  height: 100%;
  cursor: pointer;
  list-style: none;
}

summary::-webkit-details-marker {
  display: none;
}

summary::after {
  width: 14px;
  height: 14px;
  margin-left: 4px;
  background-color: currentColor;
  content: '';
  mask: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='14' height='14' viewBox='0 0 24 24'%3E%3Cpath d='m7 10 5 5 5-5z'/%3E%3C/svg%3E") no-repeat center / contain;
}

.menu {
  position: absolute;
  z-index: 30;
  top: calc(100% - 12px);
  right: 0;
  min-width: 160px;
  padding: 12px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 12px;
  background: var(--vp-c-bg-elv);
  box-shadow: var(--vp-shadow-3);
}

.menu a {
  display: block;
  padding: 6px 12px;
  border-radius: 6px;
  color: var(--vp-c-text-1);
  line-height: 20px;
  white-space: nowrap;
}

.menu a:hover {
  color: var(--vp-c-brand-1);
  background: var(--vp-c-default-soft);
}

.screen {
  height: auto;
  border-bottom: 1px solid var(--vp-c-divider);
}

.screen summary {
  min-height: 48px;
}

.screen .menu {
  position: static;
  padding: 0 0 12px 12px;
  border: 0;
  background: transparent;
  box-shadow: none;
}
</style>
