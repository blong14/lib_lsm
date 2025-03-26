#ifndef LSM_H
#define LSM_H

#include <stdint.h>
#include <stdbool.h>

extern void *lsm_init(void);

extern uint8_t const *lsm_read(void *const a0, uint8_t const *const a1);

extern bool lsm_write(void *const a0, uint8_t const *const a1, uint8_t const *const a2);

extern bool lsm_deinit(void *const a0);

#endif  /* LSM_H */
