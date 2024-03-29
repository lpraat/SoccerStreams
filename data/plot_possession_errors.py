from validate_possession import compute_errors_first_half, compute_errors_second_half
import matplotlib.pyplot as plt

if __name__ == '__main__':
    # errors are due to:
        # - human error
        # - tackles

    # First half
    # By taking into account the delay error is down to 45 seconds from 49

    errors = compute_errors_first_half()
    print(sum(errors.values()))
    plt.bar([key[0:4] for key in errors.keys()], errors.values(), color='g')
    plt.ylabel('Error in seconds')
    plt.xlabel('Player')
    plt.ylim(0, 60)
    plt.show()
