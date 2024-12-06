# ema.py
class ExponentialMovingAverage:
    def __init__(self, alpha=0.1):
        """
        Initialize EMA calculator.

        :param alpha: Smoothing factor (0 < alpha <= 1).
        """
        self.alpha = alpha
        self.ema_state = {}

    def calculate_ema(self, current_value, device_ip):
        """
        Calculate and update EMA for a given device.

        :param current_value: The current data point.
        :param device_ip: Unique identifier for the device (e.g., IP address).
        :return: Updated EMA value for the device.
        """
        previous_ema = self.ema_state.get(device_ip, None)
        if previous_ema is None:  # First value for this device
            ema = current_value
        else:
            ema = self.alpha * current_value + (1 - self.alpha) * previous_ema

        # Update the state
        self.ema_state[device_ip] = ema
        return ema

    def get_state(self):
        """Returns the current state of EMA for all devices."""
        return self.ema_state
