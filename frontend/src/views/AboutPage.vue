<template>
  <div class="about-page">
    <div class="about-container">
      <AboutCard />
    </div>
  </div>
</template>

<script setup lang="ts">
import AboutCard from '@/components/AboutCard.vue'
</script>

<style scoped>
.about-page {
  width: 100%;
  display: flex;
  justify-content: center;
  padding: 20px 0;
}

.about-container {
  max-width: 900px;
  width: 100%;
}

@media (max-width: 768px) {
  .about-page {
    padding: 10px 0;
  }
  
  .about-container {
    max-width: 100%;
  }
}
</style>

