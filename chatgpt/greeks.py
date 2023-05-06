import numpy as np
from math import erf


def calculate_greeks(S, K, r, q, T, sigma):
    d1 = (np.log(S/K) + (r - q + sigma**2/2)*T) / (sigma*np.sqrt(T))
    d2 = d1 - sigma*np.sqrt(T)
    N = lambda x: (1.0 + erf(x / np.sqrt(2.0))) / 2.0  # Black-Scholes cumulative distribution function

    delta = np.exp(-q*T) * N(d1)
    gamma = np.exp(-q*T) * np.exp(-d1**2/2) / (S * sigma * np.sqrt(2*np.pi*T))
    beta = np.exp(-r*T) * ((K/S)**(-1j*delta/(2*np.pi)))  # Merton's beta
    alpha = r - q - (sigma**2)/2 - (delta*np.log(S/K))/T  # Option alpha

    return delta, gamma, beta, alpha

# Example usage
S = 100    # current stock price
K = 110    # option strike price
r = 0.05   # risk-free interest rate
q = 0.02   # dividend yield
T = 1.0    # time to maturity (in years)
sigma = 0.2   # volatility

delta, gamma, beta, alpha = calculate_greeks(S, K, r, q, T, sigma)

print("Delta: %.4f" % delta)
print("Gamma: %.4f" % gamma)
print("Beta: %.4f" % beta.real)  # Only real part is meaningful
print("Alpha: %.4f" % alpha)


S = 100   # Stock price
K = 100   # Strike price
r = 0.05  # Risk-free rate
T = 1     # Time to expiration
sigma = 0.2   # Volatility
kappa = 0.3   # Mean reversion speed
theta = 0.2   # Mean reversion level
rho = -0.5    # Correlation between stock price and volatility

d1 = (np.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*np.sqrt(T))
d2 = d1 - sigma*np.sqrt(T)*(-1j)

beta = kappa - rho*sigma*1j*d1
alpha = -1j*np.log(np.exp(-r*T)*K/S) + (r - 0.5*sigma**2)*T + (beta*theta/sigma**2)*(T) \
    - ((beta**2)/(4*kappa))*np.log(1 - 2*kappa*rho*sigma*1j*d1 + kappa**2*sigma**2*d1**2)

option_price = np.exp(-alpha)*S**beta*np.exp(d2)
