<template>
  <aside class="alpha-notice" role="note" aria-label="Alpha software">
    <p class="alpha-notice-title">Alpha software</p>
    <p>
      Boomerangz is under active development. Commands, configuration, and
      on-disk state may change incompatibly between releases, and it has not
      yet seen wide production use. Keep independent backups of anything you
      protect with it.
    </p>
  </aside>
</template>

<style scoped>
.alpha-notice {
  margin: 0 0 24px;
  border: 1px solid var(--vp-custom-block-warning-border);
  border-radius: 8px;
  padding: 12px 16px;
  color: var(--vp-custom-block-warning-text);
  background-color: var(--vp-custom-block-warning-bg);
  font-size: var(--vp-custom-block-font-size);
  line-height: 24px;
}

.alpha-notice p {
  margin: 0;
}

.alpha-notice .alpha-notice-title {
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.04em;
}
</style>
